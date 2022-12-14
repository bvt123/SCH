-- nice to look at DataGrip
create or replace view OffsetsShow on cluster replicated as
select topic,
    if(next != '',toString(dateDiff(second,run,now())),'') as elapsed,
    run,
    if(p.last = 0,'',toString(p.last)) as parts,
    last.1 as last,
    if(next.1 = 0,'',toString(next.1)) as next,rows,consumer, state, hostid,o.source
from (select * from (select *,run,'Cluster' as source from ETL.Offsets union all select *,run,'Local' as source from ETL.OffsetsLocal)
             order by last desc limit 1 by topic) as o
left join (  select topic,max(modification_time) as last
        from system.parts
        join (select dst,topic from SCH.LineageDict) as l
        on database||'.'||table = dst
        group by topic,dst
        order by last desc
) as p using topic
where last < '2099-01-01 00:00:00'  -- not show upper stubs
  and topic != 'null'
order by last desc
;
