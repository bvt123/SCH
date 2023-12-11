/*
 Code for processing a block of data when it got ready after dependencies check.
 That is SQL code template with @variables@ substitution.

 Insert to VersionedCollapsingMergeTree table with getting old values from the same table.

 - replace old to new values with correct sign processing
 - projection/MV friendly insert
 - delete row for sign=-1
 - aggregate columns coming from different event streams


 source table req:
    _sign           Int8,
    _version        UInt64,
    _pos            Int64 MATERIALIZED schId(),
    _orig_pk        Tuple(col1,col2)  - PK from orig table
   ) engine = MergeTree
   ORDER BY _pos

dest table req:
    updated_at      DateTime materialized now(),
    _version        UInt64,
    sign            Int8 default 1
    ) Engine = VersionedCollapsingMergeTree(sign, _version)

 */

insert into SCH.Params
select 'DAG_VCMT',$$
set sch_topic = '@topic@',
    keeper_map_strict_mode=1,
    max_partitions_per_insert_block=0
;

-- load transformed data block
create temporary table  __new as (
    select * from (
        with (select last,next from SCH.Offsets where topic='@topic@') as _ln
        select * except ( _part ), _pos, splitByChar('_', _part) as _block
        from (@transform@)                -- join and group by could be here
        where _pos > _ln.1 -- where push down should work
          and if(_ln.2 != 0, _pos <= _ln.2, snowflakeToDateTime(_pos) < {upto:DateTime})
        order by _version desc limit 1 by _orig_pk
    )
    order by _pos limit @step@
)
;
-- check for nojobs and block mismatch
select  checkBlockSequence(groupUniqArray([toUInt32(_block[2]),toUInt32(_block[3])])) as block_seq,
       count() rows
from __new
having throwLog(block_seq, 'ERROR','Block Sequence Mismatch. It could be a high replication lag.') = ''
  and throwLog(rows = 0,'NOJOBS','') != ''
;
-- try to acquire semaphore per dest table and shard. It could be done better with ZK API.
alter table SCH.TableMutex update
    host_tag = {HID:String} ||'@tag@',
    processor = 'VCMT'
where table='@dbtable@@shard@'
  and (host_tag = '' or updated_at > now() - interval 2 hour) -- ignore stale
;
-- check semaphore acquisition and throw exception if not
select {HID:String} || '@tag@' as _host_tag
from SCH.TableMutex
where table='@dbtable@@shard@'
  and host_tag = _host_tag
having throwLog(count()!=1 , 'MUTEX', _host_tag || ' failed in taking the mutex' ) != ''
;
-- save position
insert into SCH.Offsets (topic, next, last, rows,  processor,state,hostid)
    with (select count(), max(_pos) from __new) as stat
    select topic, stat.2, last,
        stat.1                                        as rows,
        if(rows >= @step@,'FullVCMT','VCMT')          as processor,
        if(rows > 0, 'processing', 'delayed' )        as state,
        {HID:String}                                  as hostid
    from SCH.Offsets
    where topic='@topic@'
      and next = 0
settings keeper_map_strict_mode=0;

-- sync replica for dest table
set receive_timeout=300; SYSTEM SYNC REPLICA @dbtable@ ; --wait for parts fetched from other replica

-- print processing INFO for log and throw if replication still active
with (SELECT throwLog(count() > 0,'WARNING','Replication is Active. Will try again later.')
      FROM clusterAllReplicas('{cluster}',system.replication_queue) WHERE database || '.' || table = '@dbtable@'
      ) as err
select LogLine('INFO','@topic@',{query_id:String},'processing '),
  rows, toDateTime(snowflakeToDateTime(next)),
  'step:'  || toString(dateDiff(minute, snowflakeToDateTime(last), snowflakeToDateTime(next))) || 'min' ||
  ', lag:' || toString(dateDiff(minute, snowflakeToDateTime(next), now())) || 'min' as mins,
  err
from SCH.Offsets
where topic = '@topic@'
;

-- copy data from temp table to dest table with back ref and sign = 1/-1
insert into @dbtable@
with __old AS (
         SELECT *, arrayJoin([-1,1]) AS _sign from (
            select * FROM @dbtable@ final
            PREWHERE (@primary_key@) IN ( SELECT @primary_key@  FROM __new )
            -- some efforts to deal with wrong data in dbtable
            where sign = 1
            order by _version desc, updated_at desc
            limit 1 by (@primary_key@)
        )
     )
select @vcmt_columns@ ,
     __new._version                      AS _version,
     if(__old._sign = -1, -1, 1)         AS sign
from __new left join __old using (@primary_key@)
where __new._version > __old._version
  and (__new._sign != -1 or __new._sign = -1 and __old._sign = -1)
;
-- release mutex
alter table SCH.TableMutex update host_tag = '', processor = ''
where table='@dbtable@@shard@' and host_tag = {HID:String} ||'@tag@'
;
-- release offsets
insert into SCH.Offsets (topic, last, rows, processor, state)
    select topic, next, rows,processor, toString(dateDiff(minute, snowflakeToDateTime(last), snowflakeToDateTime(next))) || 'min'
    from SCH.Offsets
    where topic='@topic@'
      and next != 0
settings keeper_map_strict_mode=0
;
-- get stats from logs
select LogLine('INFO','@topic@',{query_id:String},'processed'),
    written_rows,
    formatReadableSize(memory_usage),
    formatReadableTimeDelta(query_duration_ms/1000,'milliseconds'),
    (select count() from system.part_log where query_id={query_id:String}
       and event_time > now() - interval 1 hour), ' parts',
    (select sumMap(nulls) from ETL.Log where query_id={query_id:String}
       and ts > now() - interval 1 hour ) as add_info
from system.query_log
where query_id={query_id:String}
  and event_time > now() - interval 10 minute
  and type='QueryFinish'
  and query ilike 'insert into @dbtable@%'
order by event_time desc
limit 1
;
select LogLine('INFO','@topic@',{query_id:String},'processed2'),
    count(), sum(rows)
from system.part_log
where query_id={query_id:String}
  and event_time > now() - interval 10 minute
;
$$;

system reload dictionary on cluster '{cluster}' 'SCH.LineageDict' ;

--select * from system.query_log where event_time > now() - interval 10 minute and query_id='204b8228-78b4-44a6-b39a-6b63086a1c1f';