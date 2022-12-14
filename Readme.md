# Scheduler

### Preface

Incoming data is pushed to DWH by Airflow DAGs, received as Kafka streams, or could be sucked by clickhouse from source MySQL database tables by using native MySQL protocol. All data is placed into “Stage” tables, which then are read by the ETL process to produce wide Fact tables.

In case of failures in external systems data inserted into Stage tables could have duplicates due to the “at least once” delivery concept of clickhouse Kafka Engine.  

Block hashes checks and the ReplacingMT table engine is used for data deduplication.

Most of the Fact tables are produced by a Join-Transform operation from several tables coming from different OLTP databases with their individual paces and schedules. To process it correctly we should be sure that all data is already inserted into DWH Stage tables. Only after that, we can make a Join-Transform operation for the particular Fact table.

We decided to use a “natural time” to synchronize tables from different sources, considering that different OLTP systems work conformly to be able to serve customers. But we should add some delay to ETL processing to compensate for server clocks inconsistency and OLTP systems’ own processing lags.

The process of building a wide Fact table is scheduled by a tool external to Clickhouse named “Scheduler” by looking for updated timestamps of dependent source tables.  In case some Stage tables still do not receive data, building the Fact table will be postponed, till all the source tables are updated to [nearly] the same natural time.

As a “position” to process only new (unseen) rows we use a DateTime64 column with some monotonic id key: `pos = tuple(dt, id)`

In case of a process crash or some other error the data could be inserted into Fact tables partially and after retrying we could get duplicate rows.  To prevent that, all inserts should be idempotent - the very same for the next time.  Also, we may use transactions (begin/commit/rollback) but it’s still in early beta in Clickhouse.

### Templates

ETL Scheduler does the planning looking for timestamps of dependent tables and limiting the pace of Transform runs.  After all the dependencies are met, Scheduler starts clickhouse-client with SQL script read from the template files with some substitutions.

By now, we have 2 templates used by the Scheduler: ETL/TemplateStep.sql and ETL/TemplateReload.sql. The Step is used for incremental load, and the Reload is for a few small tables to use full reload on every ETL run. Both templates contain the sequence of SQL queries.

### Step

  That SQL script stores pointers to data in source and destination tables (positions) and some additional information in its own working table named ETL.Offsets.

Both the Scheduler and Step work by “topics”  - a label for a Fact table incrementally built during the ETL process.

What the instance of ETL Step for the specific Fact tables is doing? 

- Step move forward with time.  It may be a minute, hour, day, or whatever.
- First of all, Step calculates a job as a range of timestamps and ids.
- To do that Step looks for new data in 1st table in the dependencies list by comparing this table's timestamps and ids with the “last” column data in ETL.Offsets for that particular topic.
- Also, the job is limited by the upper limit by the timestamp of the minimal dependent table update and the maximum size of the block (configured per topic).  A default block limit is 1M rows. Limiting block size below max_insert_block_size clickhouse’s settings helps with inconsistency resulting from crashes during insert.
- When the job is defined, the upper position is calculated (as a max timestamp and id) and written to the “next” column.
- After that, we run a Transform view. That view should get data between the “last” and “next” positions from all the joined tables and output result rows with all the necessary transformations.  The result will be inserted into the destination table.
- on the end value of the next column writes to the last column. After that Step is finished, the next run will get new data from source tables.  It may be considered a transaction commit.
- If the Step process crashes somewhere,  the Scheduler will run it again with the very same  “last” and “next” positions, not recalculate new ones.  If the coming data will not have old timestamps, that technics will ensure the insert is idempotent, and duplicated parts will be skipped by hash sum check.

### Reload

This template does the following:

- drops if exist and creates auxillary ETL.<TableName>2 table
- fills it with data selected from a transform view
- exhanges auxillary and main tables
- writes to Offsets table

### ETL.Offsets

Stores both the last updated timestamp for sources and already processed position for destination tables. The Scheduler process uses those timestamps to start the process of building the destination table. That process uses timestamps to calculate a job - how much source data will be processed.

```sql
CREATE TABLE ETL.Offsets
(
    `topic` LowCardinality(String),
    `last` Tuple(DateTime64(3), UInt64),
    `rows` UInt32,
    `next` Tuple(DateTime64(3), UInt64),
    `run` DateTime64(3) MATERIALIZED now64(3),
    `consumer` LowCardinality(String),
    `state` LowCardinality(String),
    `hostid` LowCardinality(String)
)
ENGINE = KeeperMap('/ETL/Offsets')
PRIMARY KEY topic;
```

Table data is placed into ZooKeeper to be able to use the order and replication features of ZK.

ETL.Offsets should be initialized at least for the “last” column for every processed topic.

- topic - could be a source table name, dest table name, or anything else.  It’s just a name or tag.   It’s possible to read one source table with two different topics (with their offsets)
- last - tuple with (time, id) of last processed data
- next - tuple with (time, id) - limiting the amount of data planned to be processed
- run - last modification timestamp for information and finding problems.
- consumer - name of the process that promotes offset (also for finding problems)
- state - any useful information from the working process.
- hostid - the id of the host running the job for the topic. The topic should be processed by only one process at a time. Else - the MUTEX exception is generated. To clear mutex, just set an empty string to the hostid column.

There are two Offsets tables - clusterized (KeeperMap) and local (EmbeddedRocksDB). 

### Lineage

All tables used in DWH transformations are described in the Lineage table. We could set up a fixed delay for processing (when data in the OLTP system is mutating fast after creation) or add dependencies to other tables, to be sure that their data had come to DWH’s Stage tables. We also should add here every table (dst and topic fields) used as a source in ETL processes controlled by the Scheduler. We need it because the Scheduler resolves source table names by the corresponding topics.

```sql
create table LineageSource (
    dst         varchar(255) charset utf8 ,
    topic       varchar(255) charset utf8  ,
    depends_on  varchar(1024) charset utf8 ,
    template    varchar(255) charset utf8,
    delay       varchar(255) charset utf8,
    `repeat`      varchar(255) charset utf8,
    maxstep     varchar(255) charset utf8,
    `comment`     varchar(255) charset utf8,
    `before`      varchar(255) charset utf8,
    `after`       varchar(255) charset utf8
);
```

- dst - the name of the produced destination table
- topic - the tag of the transformation process
- depends_on - comma-separated list of tables needed to produce dst.
- template - a reference to SQL script template used to process that topic
- delay - minimumx time the newest data from the main source table will wait for processing (for clock inconsistency compensation and OLTP lags)
- repeat - the minimum time between process runs (could run faster on heavy data stream)
- before and after - additional SQL statements (semicolon delimited) to insert into a processed script.

### Source

- any table used as the main source for the ETL process should have at least two monotonically increasing  columns with fixed names :
    - pos DateTime64
    - id  Uint64
- that columns should be indexed - be in “order by” or a usable skip index (correlated with the “order by” columns) as the ETL process selects data as where (pos, id) between …
- Better set a TTL for such a table to 2-3 months.
- Partitioning could be used or not.

### Transform

For each Fact table, we should prepare a Transform view named ETL.TopicTransform.  Step scripts will execute that view and should provide data to insert into a Fact table.

Transform View reads 1st table in the dependencies list with where condition between the last and next positions from ETL.Offsets for the current topic.  Also, that view could read any other table both in the dependency list and not, use dictionaries, and runs long enough to process the data stream.

It’s possible to use additional SQL statements to run other views before and after the Transform view.

### Logging

You should provide a logging Mat View which reads the dest Fact table, calculate some aggregates on the inserted block and write statistics to the ETL.Log table. 

```sql
create table if not exists ETL.LogStore
(
    topic             LowCardinality(String),
    ts                DateTime default now(),
    rows              UInt32,
    max_id            UInt64,
    min_ts            DateTime,
    max_ts            DateTime,
    max_lag           alias dateDiff(second , min_ts, ts),
    nulls             Map(String,UInt16),
    query_id          UUID
)
engine = MergeTree ORDER BY (topic,ts);
create table Log as LogStore ENGINE = Buffer(ETL, LogStore, 16, 10, 100, 10000, 1000000, 10000000, 100000000);
```

At least row count could and should be calculated. Other columns are optional but give more control over the ETL process:

- max_id, min_ts, max_ts - any stats that have meaning in the context of the particular Fact table
- nulls - a map with potential problems in blocks (some value is null due to left join, some values are out of boundaries or not correlated to other values)
- query_id - will give the ability to join with system.query_log and show memory usage in ETL.LogExt view

### Natural vs Synthetic timestamps


### Insert MV inconsistency

[Idempotent inserts into a materialized view](https://kb.altinity.com/altinity-kb-schema-design/materialized-views/idempotent_inserts_mv/)
