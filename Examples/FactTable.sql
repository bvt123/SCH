/*
 depends on: Agi.LedgerAccount Agi.LedgerTransaction
 dependent tables/MVs:  ETL.CustomerBalance, Fact.Casino

 */

--drop table Fact.Transaction3;
--exchange tables Fact.Transaction and Fact.Transaction3;
CREATE TABLE if not exists FactTable on cluster replicated
(
    `uuid` UUID,
    `customer` UUID,
    `credit_amount` Decimal(38, 3),
    `debit_amount` Decimal(38, 3),
    `created_at` DateTime64(3),
    `related_id` UInt64,
    `related_type` LowCardinality(String),
    `brand` LowCardinality(String),
    `currency` FixedString(3),
    `category` LowCardinality(String),
    `action` LowCardinality(String),
    `table` LowCardinality(String),
    `id` UInt64 MATERIALIZED cityHash64(uuid),
    `pos` DateTime64(3) MATERIALIZED now64(3),
    INDEX ix1 created_at TYPE minmax GRANULARITY 2,
    INDEX ix2 pos TYPE minmax GRANULARITY 2
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/replicated/Fact/Table', '{replica}')
PARTITION BY (table, toYYYYMM(created_at))
ORDER BY (related_id, uuid)
;

--insert into Fact.Transaction3 select * except ( loaded_at ) from Fact.Transaction;
set agi_topic='Transaction';
use Agi;

create or replace view ETL.TransactionTransform as
with lt as (
          select *,
              reinterpretAsUUIDReversed(base64Decode(d.uuid) )      as _uuid,
              toUInt64OrDefault(replace(d.related_id,'2006-',''))   as _related_id,
              toDecimal64OrZero(toString(d.credit_amount),3) as credit_amount,
              --d.credit_amount as credit_amount,
              toDecimal64OrZero(toString(d.debit_amount),3) as debit_amount,
              --d.debit_amount as debit_amount,
              d.related_type,d.created,d.account_uuid
          from LedgerTransaction where NextBlock()
    )
select * from (
     select _uuid                                                 as uuid,
            reinterpretAsUUIDReversed(la.user_uuid)               as customer,
            credit_amount,
            debit_amount,
            parseDateTime64BestEffortOrZero(d.created)            as created_at,
            _related_id                                           as related_id,
            replaceRegexpOne(d.related_type,'(_\d+)|(_-\d+)','')  as related_type,
            lt.brand                                              as brand,
            la.currency                                           as currency
    from lt
    left join LedgerAccount as la
    on la.key = base64Decode(d.account_uuid) || '-' || lt.brand
    settings join_algorithm='direct'
) as lta
semi left join RelatedTypeLedger as rt using related_type
where  -- row deduplication
       (related_id,uuid) not in (
            select related_id,uuid from Fact.Transaction where (related_id,uuid) in
                (select _related_id,_uuid from lt )
       )
;

drop table if exists ETL.__TransactionLog;
create materialized view if not exists ETL.__TransactionLog to ETL.Log as
select 'Transaction' as topic,
       count()           as rows,
       max(related_id)   as max_id,
       max(created_at)   as max_ts,
       min(created_at)   as min_ts,
       mapFilter((k, v) -> (v != 0),map(
           'created_at',countIf(created_at = 0),
           'customer',countIf(customer = toUUID('00000000-0000-0000-0000-000000000000')),
           'category',countIf(category = '')
       )) as nulls,
       queryID()  as query_id
from Fact.Transaction;
