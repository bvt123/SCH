create or replace function throwLog as (cond,tag,mess) ->
    if(throwIf(cond, '|' || tag || '\t' || mess || '|')=0,'','');

use ETL;

drop table if exists LogNull on cluster replicated ;
create table if not exists ETL.LogNull on cluster replicated
(
    topic             LowCardinality(String),
    ts                DateTime default now(),
    rows              UInt32,
    max_id            UInt64,
    min_ts            DateTime,
    max_ts            DateTime,
    nulls             Map(String,UInt16),
    query_id          UUID
)
engine = Null
;

drop table if exists LogStore on cluster replicated ;
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
    query_ids         Array(UUID)
)
engine = MergeTree
ORDER BY (topic,ts)
partition by toDate(ts)
TTL ts + interval 1 month ;

drop table if  exists  __LogStore on cluster replicated;
create materialized view if not exists __LogStore on cluster replicated to LogStore  as
select topic,
    sum(rows) as rows,
    max(max_id) as max_id,
    min(min_ts) as min_ts,
    max(max_ts) as max_ts,
    sumMap(nulls) as nulls,
    arrayFilter(x->x != toUUID('00000000-0000-0000-0000-000000000000'),groupArray(query_id)) as query_ids
from LogNull
group by topic;

drop table Log on cluster replicated;
create table if not exists Log on cluster replicated
    as LogNull ENGINE = Buffer(ETL, LogNull, 1, 10, 600, 10000, 1000000, 10000000, 100000000);

create or replace view LogExt on cluster replicated as
select event_time,topic,
    round(query_duration_ms/1000) as dur,
    rows,
    written_rows as written,
    round(rows/dur) as rate,
    formatReadableSize(memory_usage) memory,
    max_ts,
    formatReadableQuantity(read_rows) read
    --formatReadableQuantity(written_rows) written
from (select topic,rows,max_ts,arrayJoin(query_ids) as query_id
      from ETL.LogStore
      where query_id != toUUID('00000000-0000-0000-0000-000000000000')
      order by ts desc limit 1000000
     ) as sl
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