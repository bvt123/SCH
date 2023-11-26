create or replace view SCH.ProcessTemplates on cluster '{cluster}' as
WITH dictGet('SCH.systemViews', 'create', trimBoth(x)) AS viewcode,
    map('table',    splitByChar('#', L.table)[1],
        'src',      dep[1],
        'before',   before,
        'after',    after,
        'maxstep',  maxstep,
        'delay',    toString(delay),
        'repeat',   toString(repeat),
        'insert_into_table', arrayStringConcat(arrayMap(x ->
            concat('insert into ', splitByChar('#', L.table)[1], ' ', viewcode, '; \n'),
                                   splitByChar(',', L.transforms))),
        'insert_into_table_new', arrayStringConcat(arrayMap(x ->
            concat('insert into ', splitByChar('#', L.table)[1], '_new ', viewcode, '; \n'),
                                   splitByChar(',', L.transforms))),
        'cluster','{cluster}'
        ) AS subst
SELECT L.table,
    arrayMap(x -> trimBoth(x), splitByChar(',', L.depends_on))     AS dep,
    if(delay = '', 0, parseTimeDelta(delay))                       AS delay,
    if(repeat = '', 3600, parseTimeDelta(repeat))                  AS repeat,
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
