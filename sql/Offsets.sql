/*
 migration:

stop scheduler
stop all kafka consumers (and check!)
recreate and copy Offsets and OffsetsLocal (on both nodes)
change the code and apply object changes to database (both servers)
reload dicts
start all kafka consumers
start new scheduler
commit new scheduler code

 */
use SCH;

create or replace function bvtTableDependencies on cluster replicated as (t,_delay) -> (
    select least(min(last.1),now() - interval _delay second), argMin(topic,last.1)
    from (select *
          from (select * from ETL.Offsets union all select * from ETL.OffsetsLocal)
          where length(splitByChar(':',topic) as topic_host) = 1 or topic_host[2] = getMacro('shard')
          order by last desc limit 1 by topic)
    where has(arrayMap(x->dictGet('SCH.LineageDst','topic',x),dictGet('SCH.LineageDict','depends_on',t)),splitByChar(':',topic)[1])
);

create or replace function NextBlock as () -> (
    -- _pos > ((select last,next from ETL.Offsets where topic=t) as _ln).1 and _pos <= _ln.2
    -- index not working while using tuples at v22.9, so do it in a hard way
    pos > ((
        select last,next from Offsets where topic=getSetting('agi_topic')
         and (length(splitByChar(topic,':') as topic_host) = 1 or topic_host[2] = getMacro('shard'))
    ) as _ln).1.1 or pos = _ln.1.1  and id > _ln.1.2)
      and
    (pos < _ln.2.1 or pos = _ln.2.1  and id <= _ln.2.2)
;


/*
'clickhouse0' || toString(betslip_id % 2 + 1)  || '.verekuu.com'
clickhouse01.prod.betpawa.com

select getMacro('shard'); */

drop table Offsets on cluster replicated sync;
CREATE TABLE if not exists Offsets on cluster replicated
(
    topic       LowCardinality(String),
    last        Tuple(DateTime64(3), UInt64),
    rows        UInt32,
    next        Tuple(DateTime64(3), UInt64),
    run         DateTime64(3) MATERIALIZED now64(3),
    consumer    LowCardinality(String),
    state       LowCardinality(String),
    hostid      LowCardinality(String)
)
ENGINE = KeeperMap('/SCH/Offsets')
PRIMARY KEY topic;

create table if not exists OffsetsLocal on cluster replicated as Offsets ENGINE = EmbeddedRocksDB primary key (topic);
