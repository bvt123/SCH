-- check if we are already processing that topic somewhere and print a log line
set sch_topic='';
create or replace view SCH.OffsetsCheck on cluster '{cluster}' as

with  getSetting('sch_topic') as _topic,
  (select rows, snowflakeToDateTime(last), snowflakeToDateTime(next), hostid, run, state from SCH.Offsets where topic = _topic) as off,
  splitByChar(':',getSetting('log_comment'))[1] as hostid

select now() as ts, 'INFO' as level,
  _topic || if(shard != '', '-', '') || (splitByChar(':',getSetting('log_comment'))[2] as shard),
  'processing', off.1, toDateTime(off.3) as topic,
  'step:'  || toString(dateDiff(minute, off.2, off.3)) || 'min' ||
  ', lag:' || toString(dateDiff(minute, off.3, now())) || 'min' as mins,

throwLog((select count() from system.processes
          where Settings['sch_topic'] = '''' || _topic ||'''' and query_id != query_id()) > 0,
          'ERROR','other process is serving ' || _topic) as err1,
throwLog(off.1 = 0,'NOJOBS',off.6)  as err2,
throwLog(off.4 not in ['', hostid ] and off.5 > now() - interval 3 hour,
                 'MUTEX', 'host ' || hostid || ' failed in taking the mutex' )  as err3
;

create or replace function checkBlockSequence as (arr) ->
-- input: [array of array[start,end]]
-- output: bool
    arraySum(
        (arrayFilter(x,y->y%2=0,
            (arrayPopFront
                (arrayDifference
                    (arrayFlatten(arraySort(arr))
                )
            ) as s),
            arrayEnumerate(s) ) as t
        )
    ) != length(t)
;