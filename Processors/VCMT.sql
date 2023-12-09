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
set sch_topic = '@topic@';
--set log_comment='aafaf';  -- for debug

set receive_timeout=300; SYSTEM SYNC REPLICA @dbtable@ ; --wait for parts fetched from other replica
SELECT throwLog(count() > 0,'WARNING','Replication is Active. Will try again later.') FROM clusterAllReplicas('@cluster@',system.replication_queue) WHERE database || '.' || table = '@dbtable@';

set max_partitions_per_insert_block=0;
insert into SCH.Offsets (topic, next, last, rows,  processor,state,hostid)
    with ( select last from SCH.Offsets where topic='@topic@' ) as _last,
         data as ( select _pos, splitByChar('_', _part) as block
              from @source@
              where _pos > _last and snowflakeToDateTime(_pos) < {upto:DateTime}
             ),
    (select count(), max(_pos) from (select _pos from data order by _pos limit @step@)) as stat,
    (select checkBlockSequence(groupUniqArray([toUInt32(block[2]),toUInt32(block[3])])) from data ) as check
    select topic, stat.2, last,
        stat.1                                        as rows,
        if(rows >= @step@,'FullStep','Step')          as processor,
        if(rows > 0, 'processing', 'delayed' )        as state,
        splitByChar(':',getSetting('log_comment'))[1] as hostid
    from SCH.Offsets
    where topic='@topic@'
      and next = 0
      and throwLog(check, 'ERROR','Block Sequence Mismatch. It could be a high replication lag.') = ''
;

select * from SCH.OffsetsCheck;     -- check conditions and throw nojobs to run again later

create temporary table  __new as (
    with (select last,next from SCH.Offsets where topic='@topic@') as _ln
    select * from (@transform@)                -- join and group by could be here
    where _pos > _ln.1 and _pos <= _ln.2       -- where push down should work
    order by _version desc limit 1 by _orig_pk
);

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
select @columns@ ,
     __new._version                      AS _version,
     if(__old._sign = -1, -1, 1)         AS sign
from __new left join __old using (@primary_key@)
where __new._version > __old._version
  and (__new._sign != -1 or __new._sign = -1 and __old._sign = -1)
;

select now(), 'INFO','@topic@' || '-' || splitByChar(':',getSetting('log_comment'))[2],
    'processed',
    written_rows,
    formatReadableSize(memory_usage),
    formatReadableTimeDelta(query_duration_ms/1000,'milliseconds')
from system.query_log
where query_id={uuid:String}
  and event_time > now() - interval 1 minute
  and type='QueryFinish'
  and query ilike 'insert into @dbtable@%'
;

insert into SCH.Offsets (topic, last, rows, processor, state)
    select topic, next, rows,processor, toString(dateDiff(minute, snowflakeToDateTime(last), snowflakeToDateTime(next))) || 'min'
    from SCH.Offsets
    where topic='@topic@'
      and next != 0
;

$$;

system reload dictionary on cluster '{cluster}' 'SCH.LineageDict' ;
