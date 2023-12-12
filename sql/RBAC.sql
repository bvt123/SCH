ALTER SETTINGS PROFILE 'ingest' SETTINGS
    deduplicate_blocks_in_dependent_materialized_views=1,
    min_insert_block_size_rows_for_materialized_views=10000,
    throw_on_max_partitions_per_insert_block=0
    --, optimize_on_insert=0  -- not dedublicate on insert
;

CREATE USER OR REPLACE 'scheduler' IDENTIFIED WITH sha256_hash BY '***' HOST IP '::/8' SETTINGS PROFILE 'ingest';

grant SELECT, INSERT, CREATE TABLE, CREATE DATABASE, DROP DATABASE, SYSTEM SYNC REPLICA on Stage.* to scheduler;
grant SELECT, INSERT, ALTER DELETE, OPTIMIZE,SYSTEM SYNC REPLICA on Fact.*    to scheduler;
grant SELECT, INSERT, dictGet,SYSTEM SYNC REPLICA on Dim.*                    to scheduler;
grant SELECT, INSERT on ETL.*                                                 to scheduler;
grant SELECT, INSERT,dictGet,ALTER UPDATE on SCH.*                            to scheduler;
grant SELECT, INSERT, CREATE DATABASE on timecamp.*                           to scheduler;
grant SELECT on mysql.*                                                       to scheduler;
grant REMOTE ON *.*                                                           to scheduler;
grant SHOW DATABASES ON system.*                                              to scheduler;
