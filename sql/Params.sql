drop table Params on cluster replicated sync;
--create table if not exists SCH.Params on cluster replicated
create table if not exists bvt.Params
(
    key String,
    v String,
    updated_at DateTime materialized now(),
    updated_by String materialized user()
)
-- engine = ReplicatedMergeTree('/clickhouse/replicated/SCH/Params', '{replica}')
engine = MergeTree
order by key;

CREATE OR REPLACE FUNCTION getSCHParam on cluster replicated as (k) -> (select argMax(v,updated_at) from SCH.Params where key=k);
