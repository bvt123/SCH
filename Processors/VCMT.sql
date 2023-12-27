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

 todo:

1. max(pos)
2. if(__old._sign = -1, __old._version , __new._version )   AS _version,

 */

insert into SCH.Params
select 'DAG_VCMT',$$
set sch_topic = '@topic@',
    max_partitions_per_insert_block=0
;

-- decide what data to process and check for replication lag in source table
insert into SCH.Offsets (topic, next, last, rows,  processor,state,hostid)
    with ( select last from SCH.Offsets where topic='@topic@' ) as _last,
         data as ( select _pos, splitByChar('_', _part) as block
              from @source@
              where _pos > _last and snowflakeToDateTime(_pos)  < {upto:DateTime}
             ),
    (select count(), max(_pos) from (select _pos from data order by _pos limit @step@)) as stat,
    (select checkBlockSequence(groupUniqArray([toUInt32(block[2]),toUInt32(block[3])])) from data ) as check
    select topic,
        stat.2                                              as next,
        last,
        stat.1                                              as rows,
        if(rows >= @step@,'Full@processor@','@processor@')  as processor,
        if(rows > 0, 'processing', 'delayed' )              as state,
        {HID:String}                                        as hostid
    from SCH.Offsets
    where topic='@topic@'
      and SCH.Offsets.next = 0
      and throwLog(check, 'ERROR','Block Sequence Mismatch. It could be a high replication lag.') = ''
;
-- sync replica for dest table
set receive_timeout=300; SYSTEM SYNC REPLICA @dbtable@ ; --wait for parts fetched from other replica

-- print processing INFO for log and throw if replication still active
with (SELECT throwLog(count() > 0,'WARNING','Replication is Active. Will try again later.')
      FROM clusterAllReplicas('{cluster}',system.replication_queue) WHERE database || '.' || table = '@dbtable@'
      ) as err
select now(), 'INFO','@topic@'||'-'||{seq:String},'processing',
  rows, toDateTime(snowflakeToDateTime(next)),
  'step:'  || toString(dateDiff(minute, snowflakeToDateTime(last), snowflakeToDateTime(next))) || 'min' ||
  ', lag:' || toString(dateDiff(minute, snowflakeToDateTime(next), now())) || 'min' as mins,
  err
from SCH.Offsets
where topic = '@topic@'
;
-- execute transform view for selected block
create temporary table  __new as (
    with (select last,next,rows from SCH.Offsets where topic='@topic@') as _ln
    select * from (@transform@) -- where, joins and group by could be here
    where _pos > _ln.1 and _pos <= _ln.2   -- where pushdown to view supposed to work fine for that
      and throwLog(_ln.3 = 0,'NOJOBS','') = ''
    order by _version desc,_pos desc limit 1 by _orig_pk
)
;
-- try to acquire semaphore per dest table and shard. It could be done better with ZK API.
alter table SCH.TableMutex update
    host_tag = {HID:String} ||'@tag@',
    processor = 'VCMT'
where table='@dbtable@@shard@'
  and (host_tag = '' or updated_at > now() - interval 2 hour) -- ignore stale
settings keeper_map_strict_mode=1
;
-- check semaphore acquisition and throw exception if not
select {HID:String} || '@tag@' as _host_tag
from SCH.TableMutex
where table='@dbtable@@shard@'
  and host_tag = _host_tag
having throwLog(count()!=1 , 'MUTEX', _host_tag || ' failed in taking the mutex' ) != ''
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
     if(__old._sign = -1, __old._version , __new._version )   AS _version,
     -- __new._version                                  AS _version,
     if(__old._sign = -1, -1, 1)                     AS sign
from __new left join __old using (@primary_key@)
where if(__new._sign = -1, --__new.is_deleted,
  __old._sign = -1,                -- insert only delete row if it's found in old data
  __new._version > __old._version  -- skip duplicates for updates
)
;
-- release mutex
alter table SCH.TableMutex update host_tag = '', processor = ''
where table='@dbtable@@shard@' and host_tag = {HID:String} ||'@tag@'
settings keeper_map_strict_mode=1
;
-- release offsets
insert into SCH.Offsets (topic, last, rows, processor, state)
    select topic, next, rows,processor, toString(dateDiff(minute, snowflakeToDateTime(last), snowflakeToDateTime(next))) || 'min'
    from SCH.Offsets
    where topic='@topic@'
      and next != 0
;
-- get stats from logs
select now(), 'INFO','@topic@'||'-'||{seq:String},'processed',
    sum(rows), max(max_ts),
    formatReadableTimeDelta(dateDiff(second , (select run from SCH.Offsets where topic='@topic@'),now())),
    sumMap(nulls)
from ETL.Log
where topic='@topic@'
     and ts > now() - interval 1 hour
group by query_id
order by max(ts) desc
limit 1
;

$$;

system reload dictionary on cluster '{cluster}' 'SCH.LineageDict' ;

--select * from system.query_log where event_time > now() - interval 10 minute and query_id='204b8228-78b4-44a6-b39a-6b63086a1c1f';