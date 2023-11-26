
CREATE TABLE if not exists Fact.Example (
    id          UInt32,
    data        String,
    _pos        Int64 MATERIALIZED schId()
) ENGINE = MergeTree()
ORDER BY _pos;

set agi_topic='Example';

create or replace view ETL.ExampleTransform as
with lt as (
          select *,
              reinterpretAsUUIDReversed(base64Decode(d.uuid) )      as _uuid,
              toDecimal64OrZero(toString(d.credit_amount),3) as credit_amount,
              toDecimal64OrZero(toString(d.debit_amount),3) as debit_amount,
              d.created,d.account_uuid
          from Stage.Transaction where schBlock()
    )
select * from (
     select _uuid                                                 as uuid,
            reinterpretAsUUIDReversed(la.user_uuid)               as customer,
            credit_amount,
            debit_amount,
            parseDateTime64BestEffortOrZero(d.created)            as created_at,
            la.currency                                           as currency
    from lt
    left join Stage.Account as la
    on la.key = base64Decode(d.account_uuid) || '-' || lt.brand
    settings join_algorithm='direct'
) as lta
where  -- row deduplication
       (uuid) not in (
            select uuid from Fact.Transaction where (uuid) in (select _uuid from lt )
       )
;

drop table if exists ETL.__ExamplenLog;
create materialized view if not exists ETL.__TransactionLog to ETL.Log as
select 'Transaction' as topic,
       count()           as rows,
       max(created_at)   as max_ts,
       min(created_at)   as min_ts,
       mapFilter((k, v) -> (v != 0),map(
           'created_at',countIf(created_at = 0),
           'customer',countIf(customer = toUUID('00000000-0000-0000-0000-000000000000')),
           'category',countIf(category = '')
       )) as nulls,
       queryID()  as query_id
from Fact.Example;

--CREATE MATERIALIZED VIEW ETL.__BetSlipFlowPlacedOffsets TO SCH.Offsets AS
SELECT
    'Agi.BetSlipFlowPlaced:01:'|| getMacro('replica') AS topic,
    max((pos, id)) AS last,
    count() AS rows,
    'Kafka' AS processor
FROM Agi.BetSlipFlowPlaced;
