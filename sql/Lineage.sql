
drop table if exists SCH.Lineage on cluster '{cluster}' sync;
create table  SCH.Lineage on cluster '{cluster}'
(
    topic         String,                -- dest db.table[#tag]
    source        String,                -- main source db.table
    processor     String default 'Step',
    delay         String default '0s',   -- delay processing to compensate ingesting lag. e.x '11s+22min'
    dependencies  String,                -- delay processing  by checking update time of dependant objects (tables or dicts)
    repeat        String default '1h',
    step          String default '1000000', -- how much data to process on one Transform, could be number of rows of seconds or anything else
    time          String,                -- time of day limitations. e.x. 06:00-12:00
    comment       String,
    before        String,                -- any SQL code
    after         String,                -- any SQL code
-- for audit
    user        String materialized currentUser(),
    updated_at  DateTime materialized now()
)
engine = ReplicatedMergeTree() order by tuple() ;

--insert into SCH.Lineage(table, source) values ('Fact.Example','Stage.example');

--drop dictionary SCH.LineageDict on cluster '{cluster}';
create or replace dictionary SCH.LineageDict on cluster '{cluster}'
(
    topic       String,
    source      String,
    dependencies Array(String),
    delay      UInt16,
    repeat     UInt16,
    time       Array(UInt8),
    sql        String
) PRIMARY KEY topic
SOURCE(CLICKHOUSE(
    QUERY 'select * from SCH.ProcessTemplates'
    invalidate_query 'SELECT max(updated_at) from SCH.Lineage'
) )
LAYOUT(complex_key_hashed())
LIFETIME(300);

system reload dictionary 'SCH.LineageDict' on cluster '{cluster}';

/*
create or replace function getMaxProcessTime as (_topic,_delay) -> (
    select if((least(min(last), now() - interval _delay second) as _m) != toDateTime(0),_m,toDateTime('2100-01-01 00:00:00')),
           argMin(topic,last)
    from ( select * from (
            select splitByChar(':',topic)[1] as table, snowflakeToDateTime(last) as last from SCH.Offsets
            where length(splitByChar(':',topic) as topic_shard) = 1 or topic_shard[2] = getMacro('shard') )
              union all
            select database||'.'||name as table, last_successful_update_time as last from system.dictionaries where last > 0
    )
    where has(dictGet('SCH.LineageDict','dependencies',_topic),table)
);
 */