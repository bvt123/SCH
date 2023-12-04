-- nice to look at DataGrip
create or replace view SCH.OffsetsShow on cluster '{cluster}' as
select o.topic as t,
    if(next != '',toString(dateDiff(second,run,now())),'') as elapsed,
    mem, run,
    if(modification_time = 0,'',toString(modification_time)) as modification_time,
    last,
    if(next = 0,'',toString(snowflakeToDateTime(next))) as next,rows,processor, state, hostid
from (select topic,  splitByChar('#',splitByChar(':',topic)[1])[1] as t1,
        next,snowflakeToDateTime(last) as last,run,rows,processor, state, hostid
        from SCH.Offsets
        where last < '2099-01-01 00:00:00'  -- not show upper stubs
        and topic != 'null'
    ) as o
left join ( select database||'.'||table as t2, max(modification_time) as modification_time
            from system.parts group by t2
    ) as parts on o.t1 = parts.t2
left join ( select Settings['sch_topic'] as topic, splitByChar('#',splitByChar(':',topic)[1])[1] as t1, formatReadableSize(memory_usage) as mem
            from system.processes
            where topic != ''
    ) as proc on o.t1 = proc.t1
order by o.last desc
SETTINGS allow_experimental_analyzer = 1;

select Settings['sch_topic'] as topic,formatReadableSize(memory_usage) from system.processes
where topic != ''
;
insert into Fact.ComputerTime select * from ETL.ComputerTimeTransform;
{min_insert_block_size_rows_for_materialized_views=10000, connect_timeout_with_failover_ms=1000, receive_timeout=300, load_balancing=nearest_hostname, distributed_aggregation_memory_efficient=1, do_not_merge_across_partitions_select_final=1, os_thread_priority=2, log_queries=1, max_bytes_before_external_group_by=16106127360, max_bytes_before_external_sort=16106127360, max_memory_usage=28991029248, max_memory_usage_for_user=28991029248, log_comment=ip-172-33-26-158-21ac9e1a:29132463, send_logs_level=error, parallel_view_processing=1, max_partitions_per_insert_block=0, throw_on_max_partitions_per_insert_block=0, deduplicate_blocks_in_dependent_materialized_views=1, use_structure_from_insertion_table_in_table_functions=0, sch_topic='Fact.ComputerTime'}