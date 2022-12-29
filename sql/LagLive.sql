use SCH;

drop VIEW if exists LagLive on cluster replicated sync;
CREATE LIVE VIEW LagLive with refresh 5 on cluster replicated  as

select topic,
       ifNull(hosts.host_name,hostName()) as hostname,
       sql,
       run,last,mins,
       now()
from (
    select O1.topic          as topic,
           any(O1.processor) as processor,
           any(O1.run)       as run,
           any(O1.shard)     as shard,
           any(O1.hostid)    as hostid,
           any(O1.repeat)    as repeat,
           any(O1.sql)       as sql,
           any(O1.last)      as last,
           minIf(O2.last.1, O2.last.1 != 0) as mins
    from (  with splitByChar(':',topic)[1] as t
            select topic, run, processor, hostid,
                last.1                                              as last,
                toUInt8OrZero(splitByChar(':',topic)[2])            as shard,
                dictGet('bvt.LineageDst','sql',t)                   as sql,
                dictGet('bvt.LineageDst','repeat',t)                as repeat,
                arrayJoin(dictGet('bvt.LineageDst','depends_on',t)) as dep
            from bvt.Offsets   -- tables list to build. only cluster wide
            where sql != ''
         ) as O1
    left join ( select * from
                ( select * from bvt.Offsets
                    --union all select * from SCH.OffsetsLocal
                ) where processor != 'deleted' order by last desc limit 1 by topic
        ) as O2
    on O1.dep = O2.topic
    group by topic
) as updates
left join (select host_name,shard_num from system.clusters where cluster='sharded') as hosts
on shard = hosts.shard_num
-- where
  --and last < mins - interval 10 second
  --and (datediff(second , run, now()) > repeat or processor = 'FullStep' and hostid = '')
settings join_use_nulls=1;

