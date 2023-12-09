
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
with 'ETL.ComputerTimeTransform' as x select dictGet('SCH.SystemColumns','columns',x);

with 'Fact.ComputerTime' as x
--select arrayMap(p->trimBoth(p),splitByChar(',',dictGet('SCH.SystemTables','primary_key',x)));
select arrayStringConcat(arrayMap(column->if(has(
        arrayMap(p->trimBoth(p),splitByChar(',',dictGet('SCH.SystemTables','primary_key',x))),column),
      column,
      format('if(__old._sign = -1, __old.{0}, __new.{0}) AS {0}',column)
    ),dictGet('SCH.SystemColumns','columns',x)),',')
;

