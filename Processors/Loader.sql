/*
 For loading data from an external DB (like MySQL) we can use insert ... select technique to pull data from the remote server. The source table should have an updated_at DatimeTime column and index on it.

with (select max(updated_at) from dest) as max
select * from mysql(db1,table=XXX) where created >= max and created < now() - delay
"last" position will be updated not by the processor (it doesn't know it), but by MV connected to the dest table
step here should be the number of seconds, not rows.

source table could be any string that will be substituted by the template processor:

select * from @src@
select * from mysql(named_collection, table=@src@)
select * from mysql(@tag@, table=@src@)
The topic is always ready to run and only repeat timeout stops it from executing continuously.

 */


insert into SCH.Params
select 'DAG_Loader',$$
set sch_topic = '@topic@';
--set log_comment='aafaf';  -- for debug

set max_partitions_per_insert_block=0;

insert into @table@
with ( select snowflakeToDateTime(last) from SCH.Offsets where topic='@topic@' ) as _last
select * from mysql(@tag@, table=@src@)
where created >= _last
  and created < {upto:DateTime}
;

select now(), 'INFO','@topic@' || '-' || splitByChar(':',getSetting('log_comment'))[2],
    'processed',
    sum(rows), max(max_ts),
    formatReadableTimeDelta(dateDiff(second, (select run from SCH.Offsets where topic='@topic@'),now())),
    sumMap(nulls)
from ETL.Log
where topic='@topic@'
     and ts > now() - interval 1 hour
group by query_id
order by max(ts) desc
limit 1;
;

$$;

system reload dictionary on cluster '{cluster}' 'SCH.LineageDict' ;

