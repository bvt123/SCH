
--drop table SCH.Offsets on cluster '{cluster}' sync;
CREATE TABLE if not exists SCH.Offsets on cluster '{cluster}'
(
    topic       LowCardinality(String),
    last        Int64,
    rows        UInt32,
    next        Int64,
    run         DateTime MATERIALIZED now(),
    processor   LowCardinality(String),
    state       LowCardinality(String),
    hostid      LowCardinality(String)
)
ENGINE = KeeperMap('/SCH/Offsets')  PRIMARY KEY topic;

CREATE TABLE if not exists SCH.TableMutex on cluster '{cluster}'
(
    table       String,   -- db.table:shard_num
    host_tag    String,
-- traces of action for debug and errors fixing
    updated_at  DateTime MATERIALIZED now(),
    processor   LowCardinality(String)
)
ENGINE = KeeperMap('/SCH/TableMutex') PRIMARY KEY table;

--insert into SCH.TableMutex(table) values ('Fact.ComputerTime2');