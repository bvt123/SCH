
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
ENGINE = KeeperMap('/SCH/Offsets')
PRIMARY KEY topic;

-- why do we need local??
create table if not exists OffsetsLocal on cluster replicated as Offsets ENGINE = EmbeddedRocksDB primary key (topic);

-- why do we need it?
create or replace dictionary SCH.OffsetsDict on cluster replicated
(
    topic       String,
    last        DateTime64(3)
)
primary key topic
source (CLICKHOUSE(query '
    select splitByChar('':'',topic)[1] as topic,last.1
    from ( select * from SCH.Offsets )
    where processor != ''deleted''
      and {condition}
    order by last desc
    limit 1 by topic
'))
layout ( complex_key_direct() )
;

/*
  database.table:shard-replica#topic

 Agi.BetSlipFlowPlaced:01:01
 Agi.BetSlipFlowPlaced:01:02

SELECT getMacro('shard')|| '-' ||getMacro('replica');
SELECT getMacro('replica')||'-' ||getMacro('shard');
 */

select min(snowflakeToDateTime(last)) as t from SCH.Offsets where topic like 'Fact.BetSlip#%';

CREATE OR REPLACE FUNCTION getLineage as (tag,k) -> dictGet('SCH.LineageDst',tag,t);


select table, last,
    toUInt8OrZero(shard)                      as shard,
    getLineage('sql',table)                   as sql,
    getLineage('repeat',table)                as repeat,
    getLineage('delay',table)                 as delay,
    getLineage('time',table)                  as time,
    arrayJoin(getLineage('depends_on',table)) as dep
from ( -- get minimum position over all replicas to delay processing on big replication lags.
       -- when dropping a replica it needs to remove entry from ETL.Offsets
      with splitByChar(':',topic) as topics
      select  topics[1] as table, topics[2] as shard, min(last.1) as last
      from SCH.Offsets
      group by table,shard
    )
where sql != ''
;

with splitByChar(':',topic)[1] as t
            select topic, run, processor, hostid,
                last.1                                              as last,
                toUInt8OrZero(splitByChar(':',topic)[2])            as shard,
                dictGet('SCH.LineageDst','sql',t)                   as sql,
                dictGet('SCH.LineageDst','repeat',t)                as repeat,
                dictGet('SCH.LineageDst','delay',t)                 as delay,
                dictGet('SCH.LineageDst','time',t)                  as time,
                arrayJoin(dictGet('SCH.LineageDst','depends_on',t)) as dep
            from SCH.Offsets   -- tables list to build. only cluster wide
            where sql != ''
;

