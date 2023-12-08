create or replace view SCH.ProcessTemplates on cluster '{cluster}' as
WITH ['Step'] as processors,   -- filters only needed processors from Lineage
     map('topic',   L.topic,
        'dbtable',  (splitByChar('#', L.topic)[1] as _t),
        'tag',      splitByChar('#', L.topic)[2],
        'table',    splitByChar('.',_t)[2],
        'source',   source,
        'before',   before,
        'after',    after,
        'step',     step
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
