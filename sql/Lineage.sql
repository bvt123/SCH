use SCH;

create table if not exists Lineage on cluster replicated
(
    dst         String,
    topic       String,
    depends_on  String,
    template    String,
    delay       String,
    repeat      String,
    maxstep     String default '1000000',
    comment     String,
    before      String,
    after       String,
    user        String materialized currentUser(),
    updated_at  DateTime materialized now()
)  engine = ReplicatedMergeTree('/clickhouse/replicated/SCH/Lineage', '{replica}')
order by tuple()
;

create or replace dictionary LineageDict on cluster replicated
(
    topic      String,
    dst        String,
    depends_on Array(String),
    run_ETL    bool,
    delay      UInt16,
    repeat     UInt16,
    sql        String
) PRIMARY KEY topic
SOURCE(CLICKHOUSE(
    user 'dict'
    QUERY 'select * from SCH.ProcessTemplates'
    invalidate_query 'SELECT max(updated_at) from SCH.Lineage'
) )
LAYOUT(complex_key_hashed())
LIFETIME(300);

create or replace dictionary LineageDst on cluster replicated
(
    dst   String,
    topic String
) PRIMARY KEY dst
SOURCE(CLICKHOUSE(
    user 'dict'
    QUERY 'select argMax(dst,updated_at) as dst,  topic from SCH.Lineage where topic != '''' group by topic'
    invalidate_query 'SELECT max(updated_at) from SCH.Lineage'
))
LAYOUT(complex_key_hashed())
LIFETIME(300);

system reload dictionary 'SCH.LineageDict';
system reload dictionary 'SCH.LineageDst';
