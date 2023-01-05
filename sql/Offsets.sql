
use SCH;

create or replace function getTableDependencies on cluster replicated as (t,_delay) -> (
    select least(min(last.1),now() - interval _delay second), argMin(topic,last.1)
    from (select *
          from (select * from SCH.Offsets union all select * from SCH.OffsetsLocal)
          where length(splitByChar(':',topic) as topic_host) = 1 or topic_host[2] = getMacro('shard')
          order by last desc limit 1 by topic)
    where has(dictGet('SCH.LineageDst','depends_on',t),splitByChar(':',topic)[1])
);

create or replace function schBlock on cluster replicated as () -> (
    -- _pos > ((select last,next from ETL.Offsets where topic=t) as _ln).1 and _pos <= _ln.2
    -- index not working while using tuples at v22.9, so do it in a hard way
    pos > ((
        select last,next from SCH.Offsets where topic=getSetting('agi_topic')
         and (length(splitByChar(':',topic) as topic_host) = 1 or topic_host[2] = getMacro('shard'))
    ) as _ln).1.1 or pos = _ln.1.1  and id > _ln.1.2)
      and
    (pos < _ln.2.1 or pos = _ln.2.1  and id <= _ln.2.2)
;

--drop table SCH.Offsets on cluster replicated sync;
CREATE TABLE if not exists SCH.Offsets on cluster replicated
(
    topic       LowCardinality(String),
    last        Tuple(DateTime64(3), UInt64),
    rows        UInt32,
    next        Tuple(DateTime64(3), UInt64),
    run         DateTime64(3) MATERIALIZED now64(3),
    processor   LowCardinality(String),
    state       LowCardinality(String),
    hostid      LowCardinality(String)
)
ENGINE = KeeperMap('/SCH/Offsets3')
PRIMARY KEY topic;

-- clc -q "insert into SCH.Offsets(topic, last, rows, next, processor) format TSV" < o
truncate table Offsets;
create table if not exists OffsetsLocal on cluster replicated as Offsets ENGINE = EmbeddedRocksDB primary key (topic);

create or replace dictionary SCH.OffsetsDict on cluster replicated
(
    topic       String,
    last        DateTime64(3)
)
primary key topic
source (CLICKHOUSE(user 'dict' query '
    select splitByChar('':'',topic)[1] as topic,last.1
    from ( select * from SCH.Offsets union all select * from SCH.OffsetsLocal)
    where processor != ''deleted''
      and {condition}
    order by last desc
    limit 1 by topic
'))
layout ( complex_key_direct() )
;
