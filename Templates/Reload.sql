/*
 Full reload for small data

 Params (topic,src,dst,toString(delay),before, after,maxstep,vs.as_select):
  0.  topic from SCH.Offsets (like BetSlip)
  1.  src table
  2.  dst table
  3.  delay from now for slow down data transform
  4.  additional before sql
  5.  additional after sql
  6.  maxstep for limit batch size
  7.  select * from ETL.{topic}Transform, but Transform View is expanded from system.tables because of bug that slows down query in case: insert/view/select/direct mysql dict

 */

insert into bvt.Params
select 'TemplateReload',$$

create temporary table _now as select now() as ts;

insert into SCH.Offsets (topic, next, last, rows, consumer, state, hostid)
    select getSetting('agi_topic'), (now(),0), last, 1, 'Reload','processing',
        splitByChar(':',getSetting('log_comment'))[1] as hostid
    from SCH.Offsets
    where topic=getSetting('agi_topic') and next.1 = toDateTime64(0,3)
;

-- check if we are already processing that topic somewhere
select * from SCH.OffsetsCheck;

drop table if exists {table}_new;
create table {table}_new as {table};

-- process
{insert_into_table_new}

exchange tables {table}_new and {table};

-- write to log todo: make MV
insert into ETL.Log (topic, rows, max_id) select * from ETL.__{topic}Log ;

create temporary table _stats as
    select  sum(rows) as rows, max(max_ts) as max_ts, sumMap(nulls) as nulls
    from ETL.Log
    where topic=getSetting('agi_topic')
         and ts > now() - interval 1 hour
    group by query_id
    order by max(ts) desc
    limit 1
;

insert into SCH.Offsets (topic, last, rows, consumer)
    select topic, next, (select rows from _stats),'Reload'
    from SCH.Offsets
    where topic=getSetting('agi_topic')
      and next.1 != toDateTime64(0,3)
;

select now(), 'INFO',getSetting('agi_topic') || '-' || splitByChar(':',getSetting('log_comment'))[2],
    'processed',rows,max_ts,
    formatReadableTimeDelta(dateDiff(second , (select ts from _now),now())),
    nulls
    from _stats
;

$$;

system reload dictionary 'SCH.LineageDict' on cluster replicated;
