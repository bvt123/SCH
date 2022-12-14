use SCH;

drop VIEW if exists LagLive on cluster replicated;
CREATE LIVE VIEW LagLive with refresh 5 on cluster replicated  as
select topic,
       ifNull(hosts.host_name,hostName()) as hostname,
       sql,
      -- last.1,min_last,run,now() + interval delay second,
       now()
from (select o.topic as topic, toUInt8OrZero(splitByChar(':',o.topic)[2]) as shard,
    sql
   ,last,mins.min_last,run,delay
    from ( select topic,sql,repeat,delay,min(if(depends='',now(),last.1)) as min_last
            from (
                select topic,sql,repeat,delay,dictGet('SCH.LineageDst','topic',arrayJoin(depends_on)) as depends
                from SCH.LineageDict where run_ETL
                ) as l
            left join ( select * from
                (select * from ETL.Offsets union all select * from ETL.OffsetsLocal)
                 order by last desc limit 1 by topic) as o
            on splitByChar(':',o.topic)[1] = l.depends
            group by topic,sql,repeat,delay
          ) as mins
    join (select *,run from ETL.Offsets) as o
    on splitByChar(':',o.topic)[1] = mins.topic
    where last.1 < mins.min_last - interval 10 second
      --and (datediff(second , run, now()) > repeat or o.consumer = 'FullStep' and o.hostid = '')
) as updates
left join (select host_name,shard_num from system.clusters where cluster='sharded') as hosts
on shard = hosts.shard_num
where sql != ''
settings join_use_nulls=1;

--set agi_topic='BetSlipTest';

