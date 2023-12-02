create or replace view SCH.ProcessTemplates on cluster '{cluster}' as
WITH map('topic', L.table,
        'table',    (splitByChar('#', L.table)[1] as _t),
        'name',     splitByChar('.',_t)[2],
        'source',   source,
        'before',   before,
        'after',    after,
        'maxrows',  maxrows,
        'delay',    toString(delay),
        'repeat',   toString(repeat),
        'cluster','{cluster}'
        ) AS subst
SELECT L.table, L.source,
    arrayMap(x -> trimBoth(x), splitByChar(',', L.dependencies))   AS dependencies,
    parseTimeDelta(delay)                                          AS delay,
    parseTimeDelta(repeat)                                         AS repeat,
    extractAllGroups(time, '(\\d+)\\:(\\d+)\\-(\\d+)\\:(\\d+)')[1] AS time,
    replaceRegexpAll(replaceRegexpAll(arrayStringConcat(
        arrayMap(x -> if(has(mapKeys(subst), x), subst[x], x),
              splitByRegexp('[\\{\\}]', P.v))),
                '((/\\*([^*]|[\r\n]|(\\*+([^*/]|[\r\n])))*\\*+/)|(--.*))', ''), '([\r\n]+)',
                     ' ')                                          AS sql
FROM ( SELECT * FROM SCH.Params ORDER BY updated_at DESC LIMIT 1 BY key )                                                                                       AS P
JOIN ( SELECT * FROM (SELECT * FROM SCH.Lineage ORDER BY updated_at DESC LIMIT 1 BY table )
       WHERE processor IN ['Step']
    ) AS L
ON concat('DAG_', L.processor) = P.key;
