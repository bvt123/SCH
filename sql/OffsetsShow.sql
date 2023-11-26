-- nice to look at DataGrip
create or replace view SCH.OffsetsShow on cluster '{cluster}' as
select topic,
    if(next != '',toString(dateDiff(second,run,now())),'') as elapsed,
    run,
    if(p.last = 0,'',toString(p.last)) as parts,
    last.1 as last,
    if(next.1 = 0,'',toString(next.1)) as next,rows,processor, state, hostid,o.source
from (select topic, splitByChar('#',splitByChar(':',topic)[1])[1] as t,
        next,last,run,source,rows,processor, state, hostid
        from (select *,run,'Cluster' as source from SCH.Offsets union all select *,run,'Local' as source from SCH.OffsetsLocal)
             order by last desc limit 1 by topic) as o
left join (  select database||'.'||table as t, max(modification_time) as last
             from system.parts group by t
) as p using t
where last < '2099-01-01 00:00:00'  -- not show upper stubs
  and topic != 'null'
order by last desc
;
