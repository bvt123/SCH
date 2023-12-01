/*
 ETL Step for block of data got ready after dependencies check.
 Template for placing to SCH.Params with tag TemplateStep

 to run the code 2 settings should be set:
 - agi_topic
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
select 'TemplateOneDayStep',$$

set max_partitions_per_insert_block=1000;
insert into SCH.Offsets (topic, next, last, rows,  processor,state,hostid)
    with getTableDependencies(getSetting('agi_topic'),{delay}) as _deps,
         ( select last from SCH.Offsets where topic=getSetting('agi_topic') ) as _last,
         ( (pos > _last.1 or pos = _last.1 and id > _last.2) and pos < _deps.1 ) as _cond,
         data as ( select pos,id from {src}
                   where day = (select min(day) from {src} where _cond)
                     and _cond
                   order by pos,id limit {maxstep}
                 ),
    (select count(), max((pos,id)) from data) as stat
    select topic,
        stat.2,
        last,
        stat.1                                        as rows,
        if(rows >= {maxstep},'FullStep','Step')       as processor,
        if(rows > 0, 'processing', _deps.2 )          as state,
        splitByChar(':',getSetting('log_comment'))[1] as hostid
    from SCH.Offsets
    where topic=getSetting('agi_topic')
      and next.1 = toDateTime64(0,3)
;
/*
   non replicated idempotent insert deduplication on dependent MV not works as it should by default. but it works with token
   Scheduler code will split sql on 2 parts by {set_insert_deduplication_token}
   select cityHash64(last,next) as insert_deduplication_token from ETL.Offsets where topic = '{topic}';
   todo: find better hash function and debug
 */

-- check and throw nojobs and second runs somewhere
select * from SCH.OffsetsCheck;

-- before SQL
{before}

-- Process
{insert_into_table}

-- after SQL
{after}

select now(), 'INFO',getSetting('agi_topic') || '-' || splitByChar(':',getSetting('log_comment'))[2],
    'processed',
    sum(rows),max(max_ts),
    formatReadableTimeDelta(dateDiff(second , (select run from SCH.Offsets where topic=getSetting('agi_topic')),now())),
    sumMap(nulls)
from ETL.Log
where topic=getSetting('agi_topic')
     and ts > now() - interval 1 hour
group by query_id
order by max(ts) desc
limit 1;

insert into SCH.Offsets (topic, last, rows, processor, state)
    select topic, next, rows,processor, toString(dateDiff(minute, last.1, next.1)) || 'min'
    from SCH.Offsets
    where topic=getSetting('agi_topic')
      and next.1 != toDateTime64(0,3)
;

$$;

system reload dictionary 'SCH.LineageDst' on cluster replicated;

