## SCH (Щ) - Incremental ETL processor for Clickhouse. 

When the MatView ETL process became too complicated we need something else.

 It builds wide Fact&Dim tables from highly normalized MySQL schema.  After syncing source tables to clickhouse stage DB, it waits several minutes to be sure that all data is received and joins several tables to store the result in Fact&Dims.   It requires running a bash script that executes insert-select queries.  Such script is very small and could be rewritten into any language (PHP too).
Such ETL could solve all our hierarchical tables problems (no php code change is needed then) - I will use clickhouse hierarchical dictionaries to expand the hierarchy to flat tables.   The problem with the current approach based on Materialized Views is that MV can't wait until the dictionary reloads data.  With external bash SQL runner, it becomes possible.

### From ETL to CDC

The ETL/ELT concept typically involves periodically loading and reloading data into a data warehouse, often daily. In contrast, CDC (change data capture) involves a continuous stream of data. We are attempting to transition from the former concept to the latter by running a process that builds tables over shorter periods of time, like several minutes and tens of minutes, adding incremental data to the tables.

Incoming data is received as Kafka streams directly produced by corporate software or converted from MySQL transactional binlog by MaxWell’s Daemon. Also, data could be pushed to DWH by Airflow DAGs (written in python) or requested by clickhouse’s SQL code from source MySQL database tables. 

Data is placed into “Stage” tables, which then are read by the ETL process to produce wide Fact tables. Some data goes to Fact tables directly, without stage tables.

Most of the Fact tables are produced by a Join-Transform operation from several tables coming from different OLTP databases with their paces and schedules. To process it correctly we should be sure that all data is already inserted into DWH Stage tables. Only after that, we can make a Join-Transform operation for the particular Fact table.

The ETL code is written in SQL as Views ready for the insert-as-select query to the Fact tables.

The process of building a wide Fact table is orchestrated by a tool external to Clickhouse named “Scheduler” by looking for updated timestamps of dependent source tables.  Building the Fact table will be postponed if some Stage tables still do not receive data until all the source tables are updated to [nearly] the same natural time.  The scheduler is a bash script running an infinite loop that requests a list of destination tables ready to be processed and executes SQL statements received together with the tables list.  It does the asynchronous run of clickhouse-client with the SQL code received. 

### Kafka

- Due to Clickhouse Kafka Engine limitations, we could not do a heavy Join in standard Clickhouse’s Kafka consumer loop using Materialized Views. Possibly [it will change in the future](https://github.com/ClickHouse/ClickHouse/pull/42777) but for now, we shouldn’t do long processing in MV that reads data from Kafka Engine.
- Replicated tables receive data from Kafka via a common consumer group on all replicas
- non-replicated tables receive data from Kafka via their consumer groups, with names from the “replica_id” or “shard_id” macro setting (like clickhouse01, clickhouse02)

### Deduplication on Insert

In case of failures in external systems data inserted into Stage tables could have duplicates due to the “at least once” delivery concept of Clickhouse's Kafka Engine or other cases of retries on failures in the incoming data flow. 

All ETL processes is built with idempotency in mind.  Repeated inserts should not create duplicates in destination Fact or Dimension tables. 

There are two deduplication mechanisms:

 - Block hashes checks: Clickhouse stores a hash for every inserted block, and the Scheduler uses these hashes to identify identical blocks of data for processing by the Transform Views.
 - ReplacingMT/CollaspisingMT table engines. We look up every row in destination table before inserting, to ensure that duplicates are not produced. This process slows down inserts, but it provides more assurance against different type of problems in the data pipeline.

### Hot Standby Replicated Cluster

In a replicated cluster mode ETL processes run toward only one replica. Other replicas receive data via cluster replication: 

- most stage-level tables (ReplicatedMT)
- Fact tables and Dimension sources (ReplicatedMT)
- Offsets table (KeeperMap)

Some stage-level tables are based on the EmbeddedRocksDB engine (non-replicated by design) or with full-reload processing. Such tables receive data independently on every cluster node as in sharded mode (see below). Such tables should not be big.

### Sharded Cluster

- any table could be split into several shards by some “where expression” defined in the Transform view or by processing already sharded source tables received from Kafka partitions or the previous ETL step.
- “sharding key”  could be defined in a Transform View to skip some data not related to the shard - e.x. jumpConsistentHash(id,2)
- every shard should have all the dictionaries and tables needed for join-transforms.
- The Scheduler calls the same Transform View for every shard
- SCH.Offsets table store “topic" position for every shard independently.
- The scheduler runs Transform view on every server in the shard group.
