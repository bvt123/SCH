/*
 Full reload for small data

 Params (topic,src,dst,toString(delay),before, after,maxstep,vs.as_select):
  0.  topic from ETL.Offsets (like BetSlip)
  1.  src table
  2.  dst table
  3.  delay from now for slow down data transform
  4.  additional before sql
  5.  additional after sql
  6.  maxstep for limit batch size
  7.  select * from ETL.{topic}Transform, but Transform View is expanded from system.tables because of bug that slows down query in case: insert/view/select/direct mysql dict

 */

insert into ETL.Params
select 'TemplateReload',$$

create temporary table _now as select now() as ts;
set agi_topic='{topic}';

insert into ETL.Offsets (topic, next, last, rows, consumer, state, hostid)
    select '{topic}', (now(),0), last, 1, 'Reload','processing',
        splitByChar(':',getSetting('log_comment'))[1] as hostid
    from ETL.Offsets
    where topic='{topic}' and next.1 = toDateTime64(0,3)
;

-- check if we are already processing that topic somewhere
select * from ETL.OffsetsCheck;

drop table if exists ETL.{topic}2;
create table ETL.{topic}2 as {dst};

-- process
insert into ETL.{topic}2 select * from ETL.{topic}Transform;

exchange tables ETL.{topic}2 and {dst};

-- write to log
insert into ETL.Log (topic, rows, max_id)
select * from ETL.__{topic}Log
;

create temporary table _stats as
    select  sum(rows) as rows, max(max_ts) as max_ts, sumMap(nulls) as nulls
    from ETL.Log
    where topic='{topic}'
         and ts > now() - interval 1 hour
    group by query_id
    order by max(ts) desc
    limit 1
;

insert into ETL.Offsets (topic, last, rows, consumer)
    select topic, next, (select rows from _stats),'Reload'
    from ETL.Offsets
    where topic='{topic}'
      and next.1 != toDateTime64(0,3)
;

select now(), 'INFO','{topic}' || '-' || splitByChar(':',getSetting('log_comment'))[2],
    'processed',rows,max_ts,
    formatReadableTimeDelta(dateDiff(second , (select ts from _now),now())),
    nulls
    from _stats
;

$$;

system reload dictionary 'ETL.LineageDict';
