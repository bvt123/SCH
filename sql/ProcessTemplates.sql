--create or replace view SCH.ProcessTemplates on cluster '{cluster}' as
WITH ['Step','VCMT'] as processors,   -- filters only needed processors from Lineage
     splitByChar('#', L.topic)[1] as dbtable,
     dictGet('SCH.SystemTables','as_select',L.transform) as viewcode,
     dictGet('SCH.SystemTables','primary_key',dbtable) as primary_key,
     arrayStringConcat(arrayMap(column->if(
         has(arrayMap(p
         ->trimBoth(p),splitByChar(',',primary_key)),column)
         or not has(dictGet('SCH.SystemColumns','columns',L.transform),column),
            format('__old.{0} AS {0}',column),
            format('if(__old._sign = -1, __old.{0}, __new.{0}) AS {0}',column)
      ),dictGet('SCH.SystemColumns','columns',dbtable)),',') as columns,
     map('topic',   L.topic,
        'dbtable',  dbtable,
        'tag',      splitByChar('#', L.topic)[2],
        'table',    splitByChar('.',dbtable)[2],
        'source',   source,
        'before',   before,
        'after',    after,
        'step',     step,
        'transform',  viewcode,         -- view code from transform column
        'primary_key', primary_key,     -- dest table pk from system.tables
        'columns',    columns           -- dest table columns from system.tables
     ) AS subst
SELECT L.topic, L.source,
    arrayMap(x -> trimBoth(x), splitByChar(',', L.dependencies))   AS dependencies,
    parseTimeDelta(delay)                                          AS delay,
    parseTimeDelta(repeat)                                         AS repeat,
    extractAllGroups(time, '(\\d+)\\:(\\d+)\\-(\\d+)\\:(\\d+)')[1] AS time,
    replaceRegexpAll(replaceRegexpAll(arrayStringConcat(
        arrayMap(x -> if(has(mapKeys(subst), x), subst[x], x ),
              splitByRegexp('[@]', P.v))),                          -- delimiter chars set here
                '((/\\*([^*]|[\r\n]|(\\*+([^*/]|[\r\n])))*\\*+/)|(--.*))', ''), '([\r\n]+)',
                     ' ')                                          AS sql
FROM ( SELECT * FROM SCH.Params ORDER BY updated_at DESC LIMIT 1 BY key ) AS P
JOIN ( SELECT * FROM (SELECT * FROM SCH.Lineage ORDER BY updated_at DESC LIMIT 1 BY topic )
       WHERE has(processors,processor)
    ) AS L
ON concat('DAG_', L.processor) = P.key;
