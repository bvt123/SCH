/*
Rage load from some database via table function

  - maxstep is seconds, not rows as in Step/Rows templates
  - get position, add maxstep
  - write next to Offsets
  - checks
  - process transforms
  - log results
  - move next to last

 */

insert into SCH.Params
select 'TemplateRange',$$

insert into SCH.Offsets (topic, last, next, rows, processor,state,hostid)
select topic, last,
    (least(last.1 + interval {maxstep} second,now64() - interval {delay} second ),0),
    1,
    'Range'                                         as processor,
    'processing'                                    as state,
    splitByChar(':', getSetting('log_comment'))[1]  as hostid
from SCH.Offsets
where topic = getSetting('agi_topic')
  and next.1 = toDateTime64(0, 3)
;

-- check and throw nojobs and second runs somewhere
select * from SCH.OffsetsCheck;

-- before SQL
{before}

-- Process
{insert_into_table}

-- after SQL
{after}

select now(), 'INFO',
    getSetting('agi_topic') || '-' || splitByChar(':',getSetting('log_comment'))[2],
    'processed',0,next.1,
    formatReadableTimeDelta(dateDiff(second , run, now()))
from SCH.Offsets where topic=getSetting('agi_topic')
;

insert into SCH.Offsets (topic, last, rows, processor, state)
    select topic, next, rows,processor, toString(dateDiff(minute, last.1, next.1)) || 'min'
    from SCH.Offsets
    where topic=getSetting('agi_topic')
      and next.1 != toDateTime64(0,3)
;

$$;

system reload dictionary on cluster replicated 'SCH.LineageDst' ;

