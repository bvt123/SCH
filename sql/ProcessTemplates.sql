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
    columns     Array(Tuple(String,String))
)
PRIMARY KEY table
SOURCE(CLICKHOUSE(QUERY '
select table,groupArray((name,type))
from (
    select name,type,database || ''.'' || table as table
    from system.columns
    where name not in [''_version'',''sign'']
      and default_kind not in [''ALIAS'', ''MATERIALIZED'']
order by position)
group by table
having {condition}
'))
LAYOUT(COMPLEX_KEY_DIRECT);
-- with 'ETL.ComputerTimeTransform' as x select dictGet('SCH.SystemColumns','columns',x);

create or replace function convertedColumns as (dbtable) ->
     arrayMap(column-> multiIf(
          --  column.2 = 'Date',          format('toDate(`d.{0}`) AS {0}',column.1),
            column.2 = 'DateTime',      format('parseDateTimeBestEffortOrZero(`d.{0}`) AS {0}',column.1),
            column.2 like 'DateTime64%',format('parseDateTime64BestEffortOrZero(`d.{0}`) AS {0}',column.1),
            format('cast(`d.{0}`,\'{1}\') AS {0}',column.1,column.2)
          ),
          arrayFilter(x->x.1 not in ['_sign','_version'],dictGet('SCH.SystemColumns','columns',dbtable))
      );
--select  arrayStringConcat(convertedColumns('mysql.tc_user_log'),',') as json_columns;

create or replace function getTablePrimaryKey as (dbtable) ->
   dictGet('SCH.SystemTables','primary_key',dbtable);

--create or replace view SCH.ProcessTemplates on cluster '{cluster}' as
WITH ['Step','VCMT','BINLOG'] as processors,   -- filters only needed processors from Lineage
     splitByRegexp('[#:]', L.topic)[1] as dbtable,    -- db.table#tag:shard
     dictGet('SCH.SystemTables','as_select',L.transform) as viewcode,
     dictGet('SCH.SystemTables','primary_key',dbtable) as primary_key,
     arrayStringConcat(arrayMap(column->multiIf(
         has(arrayMap(p->trimBoth(p),splitByChar(',',primary_key)),column),
            column,
         has(dictGet('SCH.SystemColumns','columns',L.transform).1,column),
            format('if(__old._sign = -1, __old.{0}, __new.{0}) AS {0}',column),
            format('__old.{0} AS {0}',column)
      ),dictGet('SCH.SystemColumns','columns',dbtable).1),',') as vcmt_columns,
      arrayStringConcat(convertedColumns(dbtable),',') as json_columns,
     map('topic',   L.topic,
         'processor', L.processor,
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
        'json_columns', json_columns,
        'vcmt_columns', vcmt_columns           -- dest table columns from system.tables
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
