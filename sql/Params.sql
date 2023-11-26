CREATE OR REPLACE FUNCTION getSCHParam as (k) -> (select argMax(v,updated_at) from SCH.Params where key=k);

--drop table Params on cluster '{cluster}' sync;
create table if not exists SCH.Params on cluster '{cluster}'
(
    key String,
    v String,
    updated_at DateTime materialized now(),
    updated_by String materialized user()
)
engine = ReplicatedMergeTree()
order by key;
