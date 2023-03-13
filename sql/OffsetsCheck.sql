-- check if we are already processing that topic somewhere and print a log line
set agi_topic='';
create or replace view OffsetsCheck on cluster replicated as
  with (select rows, last.1, next.1,hostid,run,state from SCH.Offsets where topic = getSetting('agi_topic')) as off,
      splitByChar(':',getSetting('log_comment'))[1] as hostid
  select now() as ts, 'INFO' as level,
      getSetting('agi_topic') || if(shard != '', '-', '') || (splitByChar(':',getSetting('log_comment'))[2] as shard),
      'processing', off.1, toDateTime(off.3) as topic,
    'step:'  || toString(dateDiff(minute, off.2, off.3)) || 'min' ||
    ', lag:' || toString(dateDiff(minute, off.3, now())) || 'min' as mins,

    throwLog((select count() from system.processes
              where Settings['agi_topic'] = '''' || getSetting('agi_topic') ||'''' and query_id != query_id()) > 0,
              'ERROR','other process is serving ' || getSetting('agi_topic')) as err1,
    throwLog(off.1 = 0,'NOJOBS',off.6)  as err2,
    throwLog(off.4 not in ['', hostid ] and off.5 > now() - interval 3 hour,
                     'MUTEX', 'host ' || hostid || ' failed in taking the mutex' )  as err3
;
