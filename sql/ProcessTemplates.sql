/*

*/

CREATE or replace DICTIONARY SCH.SystemTables on cluster '{cluster}'
(
    name        String,
    as_select   String,
    primary_key String
)
PRIMARY KEY name
SOURCE(CLICKHOUSE(QUERY '
select database || ''.'' || table as name,
       as_select,  primary_key
from system.tables
'))
LAYOUT(COMPLEX_KEY_DIRECT);
--with 'ETL.ComputerTimeTransform' as x select dictGet('SCH.SystemTables','as_select',x);
--with 'Fact.ComputerTime' as x select dictGet('SCH.SystemTables','primary_key',x);

CREATE or replace DICTIONARY SCH.SystemColumns on cluster '{cluster}'
(
    table       String,
    columns     Array(String)
)
PRIMARY KEY table
SOURCE(CLICKHOUSE(QUERY '
select table,groupArray(name)
from (
    select name,database || ''.'' || table as table
    from system.columns
    where name not in [''_version'',''sign'']
      and default_kind not in [''ALIAS'', ''MATERIALIZED'']
order by position)
group by table
having {condition}
'))
LAYOUT(COMPLEX_KEY_DIRECT);
-- with 'ETL.ComputerTimeTransform' as x select dictGet('SCH.SystemColumns','columns',x);

create or replace view SCH.ProcessTemplates on cluster '{cluster}' as
WITH ['Step','VCMT'] as processors,   -- filters only needed processors from Lineage
     splitByRegexp('[#:]', L.topic)[1] as dbtable,    -- db.table#tag:shard
     dictGet('SCH.SystemTables','as_select',L.transform) as viewcode,
     dictGet('SCH.SystemTables','primary_key',dbtable) as primary_key,
     arrayStringConcat(arrayMap(column->multiIf(
         has(arrayMap(p->trimBoth(p),splitByChar(',',primary_key)),column),
            column,
         has(dictGet('SCH.SystemColumns','columns',L.transform),column),
            format('if(__old._sign = -1, __old.{0}, __new.{0}) AS {0}',column),
            format('__old.{0} AS {0}',column)
      ),dictGet('SCH.SystemColumns','columns',dbtable)),',') as vcmt_columns,
     map('topic',   L.topic,
        'dbtable',  dbtable,
        'shard',    substring(L.topic,position(L.topic,':')),
        'tag',      splitByChar(':',substring(topic,position(topic,'#')))[1],
        'table',    splitByChar('.',dbtable)[2],
        'source',   source,
        'before',   before,
        'after',    after,
        'step',     step,
        'transform',  viewcode,         -- view code from transform column
        'primary_key', primary_key,     -- dest table pk from system.tables
        'vcmt_columns',    vcmt_columns           -- dest table columns from system.tables
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
