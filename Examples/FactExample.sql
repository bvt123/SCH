/*
 Transformation from Stage/Source table to Fact table.
 Both tables required to have special columns. Read here the details -
 https://github.com/bvt123/SCH/blob/4d8f55b9f0731a2bdf3385f5ad0652ee9891e662/Processors/VCMT.sql
 */

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
ORDER BY (account_id, end_date, user_id, end_time)
SETTINGS min_age_to_force_merge_seconds=9000,min_age_to_force_merge_on_partition_only=1 ;

--drop view if exists ETL.ExampleTransform ON CLUSTER '{cluster}';
create or replace view ETL.ExampleTransform ON CLUSTER '{cluster}' as
select getAccountForUser(user_id) as account_id,
    user_id, application_id,task_id,
    entry_id, window_title_id,
    end_date, end_time,
    time_span,
    _version, _sign, _pos, _orig_pk, _part   --have to add all of them!
from view_db.example;
;
set sch_topic='Fact.Example';
drop view if exists ETL.Log_Example on cluster '{cluster}';
create materialized view ETL.Log_Example on cluster '{cluster}'
    to ETL.Log as
select getSetting('sch_topic')        as topic,
       count()         as rows,
       max(user_id)    as max_id,
       max(end_time)   as max_ts,
       min(end_time)   as min_ts,
--       if(any(sign) = 1, mapFilter((k, v) -> (v != 0 and k !='login'or k='login' and v/count() > 0.01 ),
       map(
           'plus',  countIf(sign = 1),
           'minus', countIf(sign = -1)
         ) as nulls,
       toUUIDOrDefault(queryID())  as query_id
from Fact.Example
;
