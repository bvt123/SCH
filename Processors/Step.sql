/*
 ETL Step for processing a block of data when it got ready after dependencies check.
 That is dynamic SQL code with {variables} substitution

 Before calling that code by clickhouse-client (or any other SQL code runner),
  two settings should be set:
 - sch_topic  - id of the object we are going to build
 - log_comment as hostid:runid
    hostid used for mutex exclusion if Scheduler runs on different servers
    runid is only for nice-looking Scheduler logs with correlated lines

 Params:
 {topic}   - in format DB.table#xxx .....
 {table}   - dest table
 {source}  - source table
 {maxrows} - max rows to process
 {delay}   - slow down data transform for specified amount of time - parseTimeDelta('11s+22min')
 {repeat}
 {before}                            -- before SQL
 !!{insert_into_table}                 -- Process
 {after}                             -- after SQL
 {cluster}  -  just a {cluster} string (resolved by Clickhouse config macro)

 */


insert into SCH.Params
select 'DAG_Step',$$
set sch_topic = '{topic}';
--set log_comment='aafaf';  -- for debug

-- wait and check for replication lag
set receive_timeout=300; SYSTEM SYNC REPLICA {source} ;
--SELECT throwLog(count() > 0,'WARNING','Replication is Active') FROM clusterAllReplicas('{cluster}',system.replication_queue) WHERE database || '.' || table = '{source}';

set max_partitions_per_insert_block=1000;
insert into SCH.Offsets (topic, next, last, rows,  processor,state,hostid)
    with getMaxProcessTime() as _upto,
         ( select last from SCH.Offsets where topic=getSetting('sch_topic') ) as _last,
         data as ( select _pos, splitByChar('_', _part) as block
              from {source}
              where _pos > _last and snowflakeToDateTime(_pos) < _upto.1
             ),
    (select count(), max(_pos) from (select _pos from data order by _pos limit {maxrows})) as stat,
    (select checkBlockSequence(groupUniqArray([toUInt32(block[2]),toUInt32(block[3])])) from data ) as check
    select topic, stat.2, last,
        stat.1                                        as rows,
        if(rows >= {maxrows},'FullStep','Step')       as processor,
        if(rows > 0, 'processing', _upto.2 )          as state,
        splitByChar(':',getSetting('log_comment'))[1] as hostid
    from SCH.Offsets
    where topic=getSetting('sch_topic')
      and next = 0
      and throwLog(check, 'ERROR','Block Sequence Mismatch. It could be a high replication lag.') = ''
;

select * from SCH.OffsetsCheck;     -- check conditions and throw nojobs to run again later

insert into {table} select * from ETL.{name}Transform;

select now(), 'INFO',getSetting('sch_topic') || '-' || splitByChar(':',getSetting('log_comment'))[2],
    'processed',
    sum(rows), max(max_ts),
    formatReadableTimeDelta(dateDiff(second , (select run from SCH.Offsets where topic=getSetting('sch_topic')),now())),
    sumMap(nulls)
from ETL.Log
where topic=getSetting('sch_topic')
     and ts > now() - interval 1 hour
group by query_id
order by max(ts) desc
limit 1;

insert into SCH.Offsets (topic, last, rows, processor, state)
    select topic, next, rows,processor, toString(dateDiff(minute, snowflakeToDateTime(last), snowflakeToDateTime(next))) || 'min'
    from SCH.Offsets
    where topic=getSetting('sch_topic')
      and next != 0
;

$$;

--system reload dictionary on cluster '{cluster}' 'SCH.LineageDst' ;

