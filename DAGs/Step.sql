/*
 ETL Step for block of data got ready after dependencies check.
 Template for placing to SCH.Params with tag TemplateStep

 to run the code 2 settings should be set:
 - sch_topic
 - log_comment as hostid:runid
 hostid used for mutex exclusion if Scheduler runs on different servers
 runid is only for nice-looking Scheduler logs with correlated lines

 Params
  1.  table - dest table
  2.  insert_into_table - Transform views processed to insert select statements
  3.  delay from now for slow down data transform
  4.  additional before sql
  5.  additional after sql
  6.  maxstep for limit batch size
  7.  select * from ETL.{0}Transform, but Transform View is expanded from system.tables because of bug witch slows down query in case: insert/view/select/direct mysql dict
 */


insert into SCH.Params
select 'DAG_Step',$$
set sch_topic = 'Fact.ComputerTime',log_comment='aafaf';  -- debug

-- wait and check for replication lag
set receive_timeout=300; SYSTEM SYNC REPLICA {src} ;
SELECT throwLog(count() > 0,'WARNING','Replication is Active')
FROM clusterAllReplicas('{cluster}',system.replication_queue)
WHERE database || '.' || table = '{src}';

set max_partitions_per_insert_block=1000;
insert into SCH.Offsets (topic, next, last, rows,  processor,state,hostid)
    with getTableDependencies(getSetting('sch_topic'),{delay}) as _deps,
         ( select last from SCH.Offsets where topic=getSetting('sch_topic') ) as _last,
         data as ( select _pos, splitByChar('_',_part) as block from {src}
                   where _pos > _last and snowflakeToDateTime(_pos) < _deps.1
                   order by _pos limit {maxstep}
                 ),
     ( select count(), max(_pos),
         checkBlockSequence(groupUniqArray([toUInt32(block[2]),toUInt32(block[3])]) )
       from data
     ) as stat
    select topic, stat.2, last,
        stat.1                                        as rows,
        if(rows >= {maxstep},'FullStep','Step')       as processor,
        if(rows > 0, 'processing', _deps.2 )          as state,
        splitByChar(':',getSetting('log_comment'))[1] as hostid
    from SCH.Offsets
    where topic=getSetting('sch_topic')
      and next = 0
      and throwLog(stat.3, 'ERROR','Block Sequence Mismatch. It could be too high replication lag.') = ''
;

select * from SCH.OffsetsCheck;     -- check conditions and throw nojobs to run again later

{before}                            -- before SQL

{insert_into_table}                 -- Process

{after}                             -- after SQL

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

