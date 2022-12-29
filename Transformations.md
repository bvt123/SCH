### Lineage and Topics

All tables used in DWH transformations are described in the Lineage table with a set of views for the insert-select SQL query.

We could set up a fixed delay for processing (when data in the OLTP system is mutating fast after creation) or add dependencies to other tables, to be sure that their data already had come to DWH’s Stage tables. 

We should add to Lineage a row for every table used as a source in ETL processes controlled by the Scheduler. 

```sql
create table LineageSource (
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
);
```

- table - the name of the produced destination table
- depends_on - comma-separated list of tables needed to produce dst.
- processor - a reference to SQL script template used to process that topic
- transforms - comma-separated list of views for insert-select query
- delay - minimal time the newest data from the main source table will wait for processing (to compensate for clock drift between shards and OLTP servers lags)
- repeat - the minimum time between process runs (could run faster on heavy data stream)
- before and after - additional SQL statements (semicolon delimited) to insert into a processed script.

### Transform View

For each Fact (or Dimension source) table, we should prepare one or several Transform views.  Those views  should provide all the columns to make an insert into a particular Fact table.

Transform View should reads 1st table in the dependencies list with “where condition” between the last and next positions stored in SCH.Offsets table for the current topic. The function schBlock() prepared for that.  That view could read any other table both in the dependency list and not, use dictionaries, external mysql requests and runs long enough to process all the data from current block.

It’s also possible to use additional SQL statements to run other views before and after the Transform views.

### Position

We decided to use a “natural time” to synchronize Fact building process on different source tables, considering that different OLTP systems work conformly to be able to serve customers. But we should add some delay to ETL processing to compensate for servers’ clock drift and OLTP systems’ own processing lags. We slow down the processing of the next data block to be sure that all data are on place.

As a “position” to process only new (unseen) rows from source tables we use a DateTime64 column with some monotonic id key: `pos = tuple(dt, id)`

### SCH.Offsets

That table stores both the last updated timestamp for sources and already processed position for destination tables. Those timestamps is used to calculate the condition when to start the process of building the destination table. That process also uses timestamps to calculate a job - how much source data will be processed.

```sql
CREATE TABLE SCH.Offsets
(
    `topic` LowCardinality(String),
    `last` Tuple(DateTime64(3), UInt64),
    `rows` UInt32,
    `next` Tuple(DateTime64(3), UInt64),
    `run` DateTime64(3) MATERIALIZED now64(3),
    `processor` LowCardinality(String),
    `state` LowCardinality(String),
    `hostid` LowCardinality(String)
)
ENGINE = KeeperMap('/ETL/Offsets')
PRIMARY KEY topic;
```

Table data is placed into ZooKeeper to be able to use the order and replication features of ZK.

SCH.Offsets should be initialized at least for the “last” column for every new processed topic.

- topic - an id of row.  Name is based on table name with a suffixes:
    - \#tag - to build tables from different sources with different transformations
    - :xx - for sharded processing topic - could be a source table name, dest table name, or anything else.  It’s just a name or tag.   It’s possible to read one source table with two different topics (with their offsets)
- last - tuple with (time, id) of last processed data
- next - tuple with (time, id) - limiting the amount of data planned to be processed
- run - last modification timestamp for information and finding problems.
- processor - name of the process that promotes offset (also for finding problems)
- state - any useful information from the working process.
- hostid - the id of the host running the job for the topic. The topic should be processed by only one process at a time. Else - the MUTEX exception is generated. To clear mutex, just set an empty string to the hostid column.

There are two Offsets tables - clusterized (KeeperMap) and local (EmbeddedRocksDB) to store the position for replicated and non-replicated tables.

Also the Offsets table is used to place an exclusion locks to prevent multiple copies of ETL run for same topic simultaneously.

### Jobs to run

The Scheduler bash scripts sits in infinite loop waiting for a next job from the server.  Job - is the topic (table name) to process with SQL code to run.  Logic of calculating the jobs is located in Live View named **SCH.LagLive**. It reads SCH.Offset table for last run events and  SCH.Lineage table for dependancies list.

As it said earlier the Scheduler does the planning comparing timestamps of dependent tables and limiting the pace of Transform runs.  After all the dependencies are met, Scheduler starts clickhouse-client with SQL script based on the SQL code template file with some substitutions. 

### Templates

Template for every Fact table is set by the “processor” column in the Lineage table. By now, we have 2 processors used by the Scheduler: 

- Templates/Step.sql   (for incremental load)
- Templates/Reload.sql (full reload on every ETL run for small tables)

**Step**

  That SQL script stores pointers to data of source and destination tables (positions) and some additional information in its own working table named SCH.Offsets.

Both the Scheduler and Step work by “topics”  - a label for a every Fact table incrementally built during the ETL process. 

What the instance of  Step  is doing for the specific Fact tables? 

- Step move forward with time.  It may be a minute, hour, day, or whatever.
- Step will not runs too often. set repeat column in Linage for that.
- First of all, Step calculates a job as a range of timestamps and ids.
- To do that Step looks for new data in 1st table (”base table”) in the dependencies list by comparing this table's timestamps and ids with the “last” column data in SCH.Offsets for that particular topic.
- Also, the job is limited by the upper limit by the timestamp of the minimal dependent table update and the maximum size of the block (configured per table).  A default block limit is 1M rows. Limiting block size below max_insert_block_size clickhouse’s settings helps with inconsistency resulting from crashes during insert.
- When the job is defined, the upper position is calculated (as a max timestamp and id) and written to the “next” column.
- After that, we run a Transform views (and additional before/after SQL statements). That views should read (select) data between the “last” and “next” positions from “base table” and from all other tables, and output result rows with all the necessary transformations.  The result will be inserted into the destination Fact table.
- on the end of the Step process, the value of the “next” column writes to the “last” column. If the Step is finished successfully, the next Step run will read new data from source tables.  It may be considered as a transaction commit.
- If the Step process crashes somewhere,  the Scheduler will run it again with the very same  “last” and “next” positions, not recalculating new ones.   That technics will ensure the insert is idempotent, and duplicated parts will be skipped by hash sum check,  but data to “base table” should be inserted on strict timestamp-id order.

**Reload**

This template does the following:

- drops if exist and creates auxillary TableName_new table
- fills it with data selected from a transform view
- exhanges auxillary and main tables
- writes state to Offsets table

### Source tables

- any table used as the main source for the ETL process should have at least two monotonically increasing  columns with fixed names :
    - pos DateTime64
    - id  Uint64
- that columns should be indexed - be in “order by” or a usable skip index (correlated with the “order by” columns) as the ETL process selects data as where (pos, id) between …
- Better set a TTL for such a table to 2-3 months.
- Partitioning could be used or not.

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

Demo of the logging MV

```sql
create or replace view ETL.__SupportTicketLog on cluster replicated as
select getSetting('agi_topic') as topic,
        count()   as rows,
        max(id)   as max_id
from Fact.SupportTicket;
```

Please use getSetting('agi_topic')  instead of string literal as it could be different in a sharded environment.