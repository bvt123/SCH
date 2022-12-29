create or replace function throwLog as (cond,tag,mess) ->
    if(throwIf(cond, '|' || tag || '\t' || mess || '|')=0,'','');

use ETL;

--drop table if exists Log;
create table if not exists ETL.LogStore on cluster replicated
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
engine = MergeTree ORDER BY (topic,ts)
settings non_replicated_deduplication_window=100;

create table if not exists Log on cluster replicated
    as LogStore ENGINE = Buffer(ETL, LogStore, 16, 10, 100, 10000, 1000000, 10000000, 100000000);

create or replace view LogExt as
select event_time,topic,
    round(query_duration_ms/1000) as dur,
    rows,
    written_rows as written,
    round(rows/dur) as rate,
    formatReadableSize(memory_usage) memory,
    max_ts,
    formatReadableQuantity(read_rows) read
    --formatReadableQuantity(written_rows) written
from (select topic,rows,query_id,max_ts from ETL.Log where query_id != toUUID('00000000-0000-0000-0000-000000000000') order by ts desc limit 1000000) as sl
join (
    select toUUID(query_id) as query_id,event_time,
        query_duration_ms,read_rows,written_rows,memory_usage
    from system.query_log
    where event_time > now() - interval 2 day and type = 'QueryFinish'
    )  as ql
using query_id
order by event_time desc
;

--drop table if exists ErrLogStore;
create table if not exists ErrLogStore (
    ts DateTime default now(),
    topic LowCardinality(String),
    err String
) engine = MergeTree order by ts;

drop table if exists ErrLog;
create table if not exists ErrLog as ErrLogStore
ENGINE = Buffer(ETL, ErrLogStore, 16, 10, 100, 10000, 1000000, 10000000, 100000000);

create or replace view ErrLogExt as
select max(ts) as time,
    topic,
    count() c,
    splitByString('DB::Exception: ',err)[-1] as error
from ErrLog
where err not like 'NOJOBS%'
  and ts > now() - interval 1 day
group by error,topic
order by time desc
;