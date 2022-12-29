## SCH (Щ) project
### Preface

Incoming data is received as Kafka streams directly produced by corporate software or converted from MySQL transactional binlog by MaxWell’s Daemon. Also data could be pushed to DWH by Airflow DAGs (written on python) or requested by clickhouse’s SQL code from source MySQL database tables. 

Data is placed into “Stage” tables, which then are read by the ETL process to produce wide Fact tables. Some data goes to Fact tables directly, without stage tables.

Most of the Fact tables are produced by a Join-Transform operation from several tables coming from different OLTP databases with their individual paces and schedules. To process it correctly we should be sure that all data is already inserted into DWH Stage tables. Only after that, we can make a Join-Transform operation for the particular Fact table.

The ETL code is written in SQL as Views ready for the insert-as-select query to the Fact tables.

The process of building a wide Fact table is orchestrated by a tool external to Clickhouse named “Scheduler” by looking for updated timestamps of dependent source tables.  If some Stage tables still do not receive data, building the Fact table will be postponed until all the source tables are updated to [nearly] the same natural time.  The scheduler is a bash script running an infinite loop that requests a list of destination tables ready to be processed and executes SQL statements received together with the tables list.  It does the asynchronous run of clickhouse-client with the SQL code received. 

In case of failures in external systems data inserted into Stage tables could have duplicates due to the “at least once” delivery concept of clickhouse Kafka Engine or other cases of retries on failures in the incoming data flow. All the DWH is built with idempotency in mind.  Repeated inserts will not create duplicates. Block hashes checks and the ReplacingMT/CollaspisingMT table engines are used for data deduplication while inserting to Fact tables. The Scheduler selects the very same blocks of data to process by Transform Views.

Analysts and other people use Redash as a main instrument for working with data.  Redash runs on it own server/pod and store working data in Postgress DBMS.   Some users have direct access to Clickhouse cluster to run SQL queries.

MySQL and Postgress Databases  witch are used as a storage for DWH tools (AirFlow and Redash) should be backed up and/or replicated to get high availability.

### Data Sources

- Kafka topic (for corporate data streams already switched to Kafka broker)
    - Coupons
    - login-events
- MySQL via MaxWell’s Daemon and Kafka
- direct MySQL data request
- external HTTP push (Wazdan)
- external HTTP get (currency rates, google analytics)

### Hot Standby Replicated Cluster

(for stability and performance)

In a replicated cluster mode ETL processes run toward only one cluster node.

Other cluster nodes receive data via cluster replication: 

- most of stage-level tables (ReplicatedMT)
- Fact tables and Dimension sources (ReplicatedMT)
- Offsets table (KeeperMap)

Some stage-level tables based on the EmbeddedRocksDB engine (non-replicated by design) or with full-reload processing so they receive data independently on every cluster node as with the sharded mode (see below).

Client access (BI) could be directed to any cluster node (with or without a load balancer).

### Sharded Cluster

- any table could be split into several shards by some “where expression” defined in the Transform view or by reading already sharded source tables - from the Kafka partitions or from the previous ETL step.
- “sharding key”  could be defined in a Transform View to skip some data not related to shard - e.x. jumpConsistentHash(id,2)
- every shard should have all the dictionaries and tables needed for join-transforms.
- The Scheduler calls the same Transform View for every shard
- SCH.Offsets table store “topic" position for every shard independently.
- The scheduler runs Transform view on every server in the shard group.
- full reload tables are processed like shards - independently on all servers with full copy on every server (no sharding expression in Transform view). It’s OK, as such tables should not be big.

### Kafka

- Replicated tables receive data from Kafka via a common consumer group on all replicas (clickhouse-replicated)
- non-replicated tables receive data from Kafka via their own consumer groups, with names from “replica_id” macro setting (clickhouse01, clickhouse02)