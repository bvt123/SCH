create or replace function replacePlaceHolders on cluster replicated as (_text) -> -- words are nicer in templates than {0}
    replaceRegexpAll(replaceAll(replaceAll(replaceAll(replaceAll(replaceAll(replaceAll(replaceAll(replaceRegexpOne(
    _text,
    '(?i)SELECT\\s+\\*\\s+from\\s+ETL\\.{topic}Transform','{7}'), -- prepare to get select code from system.tables
    '{topic}'    ,'{0}'),
    '{src}'      ,'{1}'),
    '{dst}'      ,'{2}'),
    '{delay}'    ,'{3}'),
    '{before}'   ,'{4}'),
    '{after}'    ,'{5}'),
    '{maxstep}'  ,'{6}'),
    '{(.[^}]+)}'  ,'{{\1}}')   -- escape other {vars} while using format function
;

create or replace view ProcessTemplates on cluster replicated
    as
with depends_on[1] as src,
     format(replacePlaceHolders((select argMax(v,updated_at) from SCH.Params where key='TemplateStep')),
        topic,src,dst,toString(delay),before, after,maxstep,vs.as_select) as Step,
        format(replacePlaceHolders((select argMax(v,updated_at) from SCH.Params where key='TemplateReload')),
        topic,src,dst,toString(delay),before, after,maxstep,vs.as_select) as Reload
select topic,dst,
     arrayMap(x->trimBoth(x),splitByChar(',',depends_on))   as depends_on,
     template != ''                                         as run_ETL,
     if(delay  = '', 0,   parseTimeDelta(delay))            as delay,
     if(repeat = '', 3600,parseTimeDelta(repeat))           as repeat,
     if(run_ETL,
        replaceRegexpAll(
        replaceRegexpAll(
            multiIf(template = 'sql',   before || after,
                    template = 'Step', Step,
                    template = 'Reload', Reload,
            ''),
        '((/\*([^*]|[\r\n]|(\*+([^*/]|[\r\n])))*\*+/)|(--.*))',''),   -- https://blog.ostermiller.org/finding-comments-in-source-code-using-regular-expressions/
        '([\r\n]+)',' '),
        '')                                                 as sql
from (select argMax(dst,updated_at) as dst,  topic,
        argMax(depends_on,updated_at) as depends_on,
        argMax(template,updated_at) as template,
        argMax(delay,updated_at) as delay,
        argMax(repeat,updated_at) as repeat,
        argMax(maxstep,updated_at) as maxstep,
        argMax(comment,updated_at) as comment,
        argMax(before,updated_at) as before,
        argMax(after,updated_at) as after
    from Lineage
    where topic != ''
    group by topic ) as ls
join (select as_select,table from system.tables where database='ETL' and engine='View') as vs
on vs.table=ls.topic || 'Transform'
;

-- check if we are already processing that topic somewhere and print a log line
set agi_topic='';
create or replace view OffsetsCheck on cluster replicated as
  with (select rows, last.1, next.1,hostid,run,state from ETL.Offsets where topic = getSetting('agi_topic')) as off,
      splitByChar(':',getSetting('log_comment'))[1] as hostid
  select now(), 'INFO',
      getSetting('agi_topic') || '-' || splitByChar(':',getSetting('log_comment'))[2],
      'processing', off.1, toDateTime(off.3),
    'step:'  || toString(dateDiff(minute, off.2, off.3)) || 'min' ||
    ', lag:' || toString(dateDiff(minute, off.3, now())) || 'min' as mins,

    throwLog((select count() from system.processes
              where Settings['agi_topic'] = '''' || getSetting('agi_topic') ||'''' and query_id != query_id()) > 0,
              'ERROR','other process is serving ' || getSetting('agi_topic')) as err1,
    throwLog(off.1 = 0,'NOJOBS',off.6)  as err2,
    throwLog(off.4 not in ['', hostid ] and off.5 > now() - interval 3 hour,
                     'MUTEX', 'host ' || hostid || ' failed in taking the mutex' )  as err3
;
