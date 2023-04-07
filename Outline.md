## P1. Preface

### Goals

- receive incoming data “AS IS”.  OLTP team will not change anything for us (schema, datatypes, IDs). Except for IDs format.
- The report query response time should be less than 1 sec. It is good for reporting but also can be until 1 hour too for very complicated queries. No fight for milliseconds.
- All reports are optimized for aggregation (no point queries).
- build wide tables  for faster queries - that is a Clickhouse way
- support for incremental updates
- support for mutated data
- survive unexpected schema changes
- different data sources (External Push, Kafka Pull, MySQL/Postgress Pull)
- tens of minutes delay in processing
- easy to backup
- all transformation logic should be inside Clickhouse
- most code should be written on Clickhouse SQL (to utilize CLickhouse parallel execution)

## P2. (Theory and Problems)

### Limitations

- Most businesses do have not too much data (say 1Tb of a single copy of zipped active data)
    - [Big Data is Dead](https://motherduck.com/blog/big-data-is-dead/)
- storage is very cheap, RAM is not, ETL process should work on 64Gb Instances
- not-so-big ingestion rates  (what exactly?) to have time to process data

### From ETL to CDC

- How long are you going to wait for fresh data before doing an analytical report?

The ETL/ELT concept typically involves periodically loading and reloading data into a data warehouse, often daily. In contrast, CDC (change data capture) involves a continuous stream of data. We are attempting to transition from the former concept to the latter by running a process that builds tables over shorter periods of time, like several minutes and tens of minutes, adding incremental data to the tables.

### Kimball’s **Dimensional modeling**

[https://en.wikipedia.org/wiki/Dimensional_modeling](https://en.wikipedia.org/wiki/Dimensional_modeling)

- wide Fact tables - exactly match the Clickhouse way
    - Fact table stores already joined data for a long period (sometimes forever) in columnar format
    - data are denormalized. A bunch of separate “normalized” tables goes to one Fact table.
    - may use arrays (Nested) for storing “connected tables” data in case of 1:N relations
    - Conformed Dimensions Technique speed up JOINs of two big Fact tables during the report time
- Dimensions could be presented in different ways:
    - Dictionaries with different layouts
    - LowCardinality column (degenerated dimension in Kimball terms)
    - Join Engine table (memory table suited for fast joins)
    - MergeTree Engine table (will slow down reports)

### Joins

- to build a wide table we have to join data from several sources/tables, sometimes from different OLTP systems
- the situation with Joins in Clickhouse at the 1Q of the year 2023
    - [join_algorithms](https://clickhouse.com/docs/en/operations/settings/settings#settings-join_algorithm) setting  explained with different types of joins
    - plans for the year 2023 (push down and table reordering)
- manual JOIN optimizations
    - explicit set the join algorithm
    - max_threads = 1
    - explicit set order of join for several tables (smallest tables go right and deeper to subquery)
    - Sort Join with conformed ORDER BY
    - precalculating IN list for filtering the right table
    - precalculating min/max range for filtering the right table

### Non-monotonic keys

- problems for index searches (full scans)
- problems with joins (too big hash tables)

Solutions:

- generate snowflakeid in OLTP systems
- Tuple(now64(),hostName(),rowNumberInAllBlocks()) is do the same but IDs became big
- Tuple(now(),rowNumberInAllBlocks()) takes only 8 bytes, but could produce collisions in some cases.
- Tuple(now64(),rowNumberInAllBlocks()) with an ingestion rate of 1 block per minute could work fine with many nodes because seconds and milliseconds add enough randomization.

### Duplicate problems

- source of problems
    - Kafka
    - Humans
- deduplication methods
    - idempotency (we don’t care for duplicates)
    - external deduplication (not in Clickhouse, ref to Altinity-sink)
    - on read (final and such)
    - on insert (blocks or rows)
- row level deduplication
    - slow down inserts
    - but is aggregate-friendly
    - CollapsingMT or other sign=-1 variations.

### MVs are not for Heavy Joins

- insert will not finish until all MV processed
- insert from Kafka loop is more limited in timing

Solution:

- replace joins for fast Key-Value lookups
- insert source data as-is to local clickhouse tables and join later by some background task
    - AggregatingMT merges  (group by instead of join)
    - external scheduler doing insert … select … from local_table

## P3. (Practice and Examples)

### Key-Value lookups instead of Joins

- Dictionaries with different layouts or sources. Examples for:
    - Flat/Hashed over local or remote source
    - Cached with updates
    - Direct to local MergeTree
    - MySQL/Postgres Source with join in remote DBMS
- Join Tables (no replication, need to solve it somehow)
- RocksDB (no replication, need to solve it somehow)
- KeeperMap (for a small amount of data)

### Just aggregate It!

- AggregatingMT instead of join, explanation of AggMT merges.
- all tables have to have a common key/id
- set table ORDER BY that ID, despite the most common way for Clickhouse tables, is to place low-cardinality columns first and don’t use the ID in the first place.
- examples of merges with aggregation:
    - SimpleAggregationFunction(max,..)  for Positive Numbers and Strings
    - SimpleAggregationFunction(min,..) for Start Time
    - SimpleAggregationFunction(max, Nullable()) for All Numbers
    - SimpleAggregationFunction(anyLast…) for latest data
    - SimpleAggregationFunction(groupUniqArrayArray) with discussion about duplicates, idempotency and Tuple(id,String)
- disadvantages:
    - slow read - full-scan  and final required
    - hard to set up partitioning
    - merges a little bit slower
    - random uuid in PK leads to full scans
- good as a Dictionary source
- example1: build a Customer table from different Kafka topics and a Dictionary over it

### Add index

- Let’s try to build a bigger table on the same technology. Say 1B rows
- example2: a common task of monitoring the effectiveness of Ad Channels
    - View→Click→Order
    - events received independently but with a common ID
    - group by user, campaign, source
- how make queries from a such table?
- skip indexes with final not work. the problem explanation.
- solution based on subrequest in prewhere
    - subquery first finds IDs using a skip index without FINAL, and next do a query with ID filtering
    - for a small number of IDs use IN, for a bigger - Range. Sorry dude, no optimizer. DYI.
    - randomly generated ids are a problem here, we could not make a good range filter and got a full table scan. Have to use monotonic ids like a snowflake.
- refs to skip index papers:
    - [https://clickhouse.com/docs/en/optimize/sparse-primary-indexes#using-multiple-primary-indexes](https://clickhouse.com/docs/en/optimize/sparse-primary-indexes#using-multiple-primary-indexes)
    - [https://clickhouse.com/docs/en/optimize/skipping-indexes](https://clickhouse.com/docs/en/optimize/skipping-indexes)
    - [https://altinity.com/blog/clickhouse-black-magic-skipping-indices](https://altinity.com/blog/clickhouse-black-magic-skipping-indices)
- reverse index (as a separate table)
    - good for querying a few rows
    - for many rows works not better than skip index
    - useful when impossible to do the reordering of a wide table (AggMT or too much disk space)
    - useful for complicated index expressions (e.x. on several columns)

### Join several data streams

- append-only mode, no mutated data
- example3: same as example2 but a different way
    - source table normalized and doesn’t have one common ID (but it’s still monotonic)
    - heavy join of 3 tables with range precalculation
    - introduce and explain ORDER BY optimization with toDate(timestamp) in the first place
    - query example with day filter and group by
- Join several sources by insert … select techniques initiated by an external tool.
- use plain MergeTree Engine with row-level deduplication on insert. Pros:
    - no need  FINAL or other deduplication technics during  query time
    - skip indexes will work
    - aggregation friendly
- introducing ReadyToJoin Stage tables
    - temporary storage only to have fast access to source data for building a wide Fact table
    - may be long-lived, or truncated by TTL - depends on the data
    - Stage table schema inherits a source table schema. Columns and types like in the source.
    - schema-free - source table columns stored in Object(’JSON’). New columns could be easily added without disrupting the ETL process. In some cases, type changes for source columns also survived.
    - Table schema (ORDER BY) set  for speed-up building wide table with JOINs and other techniques
    - could have duplicates, need deduplicate before JOIN
- introducing a Queue
    - one Stage table became a leader in Join (left table), other tables should be ReadToJoin
    - that table should have a sequential ID (autogenerated on insert)
    - on every step, we get a fixed rows number from the table to enable block deduplication on errors
    - store current position (like Kafka offset) in some KV storage (RocksDB, KeeperMap)
    - why reinvent Kafka? (because we don’t want to do any hard work in a Kafka insert loop)

### UPSERT for Mutated data

- what to do if the source data changes?
- introduce CollapsingMT solution
- need final or sum(sign) on report queries
- could not use skip indexes with final (but the trick with subquery via PK will help again)
- no need to make optimizations for all reports queries, see Marts section below.
- example4: extend example3 by adding multiple Orders with a mutating total amount and array with details
    - query with FINAL and reverse index subquery in prewere
    - query with a sign for totals calculation
- where from to get the old data for sign=-1 rows?
    - OLTP data stream (binlog have such)
    - request from Clickhouse table (slow down inserts)

## Part 3 (ETL Processor)

### SQL Modular programming

- definition and ref [https://en.wikipedia.org/wiki/Modular_programming](https://en.wikipedia.org/wiki/Modular_programming)
- one source file with all SQL objects
- DDL statements only (no DML here)
- create table - like data definition
- view like an interface function
- materialized view like an internal method
- source for building sql schema packages.
    - on cluster to all objects
    - if exist/if not exist to all DDL statements to easily recreate schema in cluster environment
    - drop objects (like MV) when needed

### Scheduler

- could be a simple bash loop doing insert … select
    - but better write something more sophisticated on golang/python/java
- provide dynamic SQL features and variable substitutions
- The scheduler runs a Transform View to add data to the destination table
- runs are planned based on the last update time of source tables
- async - several tables could be processed in parallel with necessary locks to prevent run simultaneously two processes toward one destination table.

### Lineage

- describe table dependency while building wide Fact tables
- used to postpone Scheduler runs until all source data will be ready to JOIN
- the synchronicity of events in OTLP and joining multiple sources in DWH
    - I decided to use the time to synchronize Fact building process on different source tables, considering that different OLTP systems work conform to be able to serve customers. We should not process with joins too early (when not all data is received) so a considerable delay in ETL processing will help to compensate for servers’ clock drift and OLTP systems’ own processing lags. We slow down the processing of the next data block to be sure that all data in all source tables are in place. DateTime part of position is used to calculate the upper limit of the data to be processed.

### Logging

- a universal approach for all tables
- create MV for every Fact table as a table “method”
- Buffer table to prevent creating too many parts
- report views over Log table

## Part 4 (Additional DWH elements)

### Marts

(buys speed for storage)

- Mart is a table prepared for report queries with WHERE filter on columns out of Fact’s ORDER BY/PK or data granularity by some Dimension.
- A separate Mart table with reordering is needed only for columns where the order differs a lot. Some columns could be in a group with a compatible order. In such case, no need to create a separate Mart for all of them - skip indexes will work fine.
- Mart could store Aggregated data with GROUP BY by some set of columns. Such tables should store much fewer data than the original table.  No need to create aggregation with a factor of less than 10, compared to the original number of rows.  Example with second/minute/hour/day/week/month/year
- Mart also could be based (read data) on several Facts and Dimensions Tables (sources).
- Mart could be “hidden” projection table or a normal table filled by MV. In this case source tables for building Mart should be “aggregation ready” - without duplicates inserted - No ReplacingMT/CollapsingMT. See https://github.com/ClickHouse/ClickHouse/issues/24778
- for mutating Fact tables with ReplacingMT/CollapsingMT Engine and a special “sign” column, it's possible to build Mart as a normal table (not projection) with numeric aggregates “fixed” on changes and deletes by summing with sign value.
- also, Marts could be built by an “insert … select” processor. In such cases, we can use RMT source tables with duplicates and remove them by FINAL modifier.

### Incremental Backups

describe incremental backup problems that arise from merges of data in old partitions. Try building partitioning for append-only mode.

Solution:

- create append-only data streams by copying new rows from Fact tables to additional Backup tables by MVs
- rotate Backup table before the backup process
- no need to backup all tables
    - Marts (it’s simpler to rebuild them)
    - Queued Stages, in most cases it’s simpler to reinject them
    - in some cases, it’s simpler to reinject ReadyToJoin Stages from OLTP source
- restore process is more complicated
    - restore schema by “schema package DDLs”
    - restore Fact tables from Backup
    - restore Stage tables by copying from OLTP sources
    - rebuild Mart tables
