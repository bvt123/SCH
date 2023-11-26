
create or replace function getTableDependencies as (t,_delay) -> (
    select least(min(snowflakeToDateTime(last) as _l), now() - interval _delay second), argMin(topic,_l)
    from (select * from SCH.Offsets
          where length(splitByChar(':',topic) as topic_host) = 1 or topic_host[2] = getMacro('shard')
          order by last desc limit 1 by topic)
    where has(dictGet('SCH.LineageDst','depends_on',t),splitByChar(':',topic)[1])
);

--drop table if exists SCH.Lineage on cluster '{cluster}' sync;
create table if not exists SCH.Lineage on cluster '{cluster}'
(
    table       String,
    depends_on  String,
    processor   String,
    transforms  String,
    delay       String,
    repeat      String,
    maxstep     Nullable(String),
    time        String,  -- in format 0:30-6:00
    comment     String,
    before      String,
    after       String,
    user        String materialized currentUser(),
    updated_at  DateTime materialized now()
)
engine = ReplicatedMergeTree() order by tuple() ;

-- clc -q "insert into SCH.Lineage(table, depends_on, processor, transforms, delay, repeat, maxstep) format TSV" < l
insert into SCH.Lineage(table, depends_on, processor, transforms, delay, repeat, maxstep) values (
  'Fact.ComputerTime','Stage.tc_user_log','Step','ETL.ComputerTimeTransform','5m','10m','1000000'
);

create or replace dictionary SCH.LineageDst on cluster '{cluster}'
(
    table       String,
    depends_on Array(String),
    delay      UInt16,
    repeat     UInt16,
    time       Array(UInt8),
    sql        String
) PRIMARY KEY table
SOURCE(CLICKHOUSE(
    QUERY 'select * from SCH.ProcessTemplates'
    invalidate_query 'SELECT max(updated_at) from SCH.Lineage'
) )
LAYOUT(complex_key_hashed())
LIFETIME(300);

system reload dictionary 'SCH.LineageDst' on cluster '{cluster}';