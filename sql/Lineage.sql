use SCH;

drop table if exists SCH.Lineage on cluster replicated sync;
create table if not exists SCH.Lineage on cluster replicated
(
    table       String,
    depends_on  String,
    processor   String,
    transforms  String,
    delay       String,
    repeat      String,
    maxstep     Nullable(String),
    comment     String,
    before      String,
    after       String,
    user        String materialized currentUser(),
    updated_at  DateTime materialized now()
)
engine = ReplicatedMergeTree('/clickhouse/replicated/SCH/Lineage2', '{replica}')
order by tuple()
;

-- clc -q "insert into SCH.Lineage(table, depends_on, processor, transforms, delay, repeat, maxstep) format TSV" < l

create or replace dictionary SCH.systemViews on cluster replicated
(
    name String,
    create String
) PRIMARY KEY name
layout(complex_key_direct)
SOURCE (CLICKHOUSE(user 'dict' query '
    select database || ''.'' || table as name,as_select as create from system.tables where engine=''View'' and {condition}
'))
;

create or replace dictionary SCH.LineageDst on cluster replicated
(
    table       String,
    depends_on Array(String),
    delay      UInt16,
    repeat     UInt16,
    sql        String
) PRIMARY KEY table
SOURCE(CLICKHOUSE(
    user 'dict'
    QUERY 'select * from SCH.ProcessTemplates'
    invalidate_query 'SELECT max(updated_at) from SCH.Lineage'
) )
LAYOUT(complex_key_hashed())
LIFETIME(300);

system reload dictionary 'SCH.LineageDst' on cluster replicated;
