use SCH;

--drop VIEW if exists LagLive on cluster replicated sync;
-- CREATE LIVE VIEW LagLive with refresh 5 -- on cluster replicated

create or replace view LagLive on cluster replicated
    as
with if(mins != 0, mins, now()) as mins_now
select topic,
       ifNull(hosts.host_name,hostName()) as hostname,
       sql,
--      run,last,mins,mins_now,repeat,
       now()
, toUInt32(now()) - toUInt32(toDateTime('2023-01-01 00:00:00'))
from (
    select O1.topic          as topic,
           any(O1.processor) as processor,
           any(O1.run)       as run,
           any(O1.shard)     as shard,
           any(O1.hostid)    as hostid,
           any(O1.repeat)    as repeat,
           any(O1.delay)     as delay,
           any(O1.sql)       as sql,
           any(O1.last)      as last,
           minIf(O2.last.1, O2.last.1 != 0) as mins
    from (  with splitByChar(':',topic)[1] as t
            select topic, run, processor, hostid,
                last.1                                              as last,
                toUInt8OrZero(splitByChar(':',topic)[2])            as shard,
                dictGet('SCH.LineageDst','sql',t)                   as sql,
                dictGet('SCH.LineageDst','repeat',t)                as repeat,
                dictGet('SCH.LineageDst','delay',t)                 as delay,
                arrayJoin(dictGet('SCH.LineageDst','depends_on',t)) as dep
            from SCH.Offsets   -- tables list to build. only cluster wide
            where sql != ''
         ) as O1
    left join ( select *,splitByChar(':',topic)[1] as t from
                 ( select * from SCH.Offsets union all select * from SCH.OffsetsLocal
                 ) where processor != 'deleted' order by last desc limit 1 by topic
        ) as O2
    on O1.dep = O2.t  -- todo: better shard_id calculation!!!
    group by topic
) as updates
left join (select host_name,shard_num from system.clusters where cluster='sharded') as hosts
on shard = hosts.shard_num
where (processor = 'FullStep' and hostid = '')
  or
      last < mins_now -- interval delay second
  and (datediff(second , run, now()) > repeat )
settings join_use_nulls=1;
