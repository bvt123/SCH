insert into SCH.Params
select 'TemplateIncremental2',$$

insert into SCH.Offsets (topic, next, last, rows, processor, state, hostid)
select getSetting('agi_topic'),
       (now(), 0),
       last,
       1,
       'Incremental2',
       'processing',
       splitByChar(':', getSetting('log_comment'))[1] as hostid
from SCH.Offsets
where topic = getSetting('agi_topic')
  and next.1 = toDateTime64(0, 3)
;

-- check if we are already processing that topic somewhere
select *
from SCH.OffsetsCheck;

-- process
{insert_into_table}



insert into SCH.Offsets (topic, last, rows, processor)
select topic, next, (select rows from _stats), 'Incremental2'
from SCH.Offsets
where topic = getSetting('agi_topic')
  and next.1 != toDateTime64(0, 3)
;

$$;

system reload dictionary on cluster replicated 'SCH.LineageDst';