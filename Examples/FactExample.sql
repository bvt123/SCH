create table Fact.Example ON CLUSTER '{cluster}'
(
    account_id      UInt32,
    user_id         UInt32,
    task_id         UInt32,

    end_date        Date CODEC (Delta,ZSTD(1)),
    end_time        DateTime,

    time_span       UInt32,
    time_span_s     alias sign*time_span,
    time_span_c     alias sign,

    updated_at      DateTime materialized now(),
    _version        UInt64,
    sign            Int8 default 1,
    PROJECTION month_user  (
        select toStartOfMonth(end_date), user_id, sum(time_span_s), sum(time_span_c)
        group by toStartOfMonth(end_date), user_id
    )
) engine = ReplicatedVersionedCollapsingMergeTree(sign, _version)
PARTITION BY toYYYYMM(end_date)
ORDER BY (account_id, end_date, mortonEncode(user_id, task_id), end_time)
SETTINGS min_age_to_force_merge_seconds=9000,min_age_to_force_merge_on_partition_only=1 ;

--drop view if exists ETL.ComputerTimeTransform ON CLUSTER '{cluster}';
set sch_topic='Fact.Example';
create or replace view ETL.ExampleTransform ON CLUSTER '{cluster}' as
with new as ( select getAccountForUser(user_id) as account_id,
                toUInt32(user_id)         user_id,
                toUInt32(task_id)         task_id,
                end_time,
                time_span,
                end_date,
                _sign, _version
             from Stage.example
             where schBlock(_pos)
             order by _version desc limit 1 by (end_date, user_id, task_id, end_time)
            ),
     old AS (
        SELECT *, arrayJoin([-1,1]) AS _sign -- could be 0 because join is left
        FROM Fact.ComputerTime
        PREWHERE (account_id, end_date, user_id, task_id, end_time) IN
         ( SELECT account_id, end_date, user_id, task_id, end_time FROM new )
     )
select account_id, user_id, application_id, task_id,
     end_date,  end_time,
     if(old._sign = -1, old.time_span, new.time_span)             AS time_span,
                                                                     _version,
     if(old._sign = -1, -1, 1)                                    AS sign
from new left join old using (end_date, user_id, application_id, end_time)
where not (sign = -1 and old._sign != -1)
;

drop view if exists ETL.Log_Example on cluster '{cluster}';
create materialized view ETL.Log_Example on cluster '{cluster}'
    to ETL.Log as
select getSetting('sch_topic')        as topic,
       count()         as rows,
       max(user_id)    as max_id,
       max(end_time)   as max_ts,
       min(end_time)   as min_ts,
/*       if(any(sign) = 1, mapFilter((k, v) -> (v != 0 and k !='login'or k='login' and v/count() > 0.01 ),map(
           'repeated',  toUInt64(count() - uniqExact(bet_slip_id)),
           'coupon',    countIf(isZeroOrNull(coupon_id))
         )), map()) as nulls,*/
       toUUIDOrDefault(queryID())  as query_id
from Fact.Example
;
