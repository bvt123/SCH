create or replace view SCH.ProcessTemplates on cluster replicated as
with dictGet('SCH.systemViews','create',trimBoth(x)) as viewcode,
     map('table'  , splitByChar('#',L.table)[1],
        'src'    , dep[1],
        'before' , before,
        'after'  , after,
        'maxstep', maxstep,
        'delay'  , toString(delay),
        'repeat' , toString(repeat),
        'insert_into_table', arrayStringConcat(
            arrayMap(x->
                'insert into ' || splitByChar('#',L.table)[1] || ' ' || viewcode || '; \n'
            ,splitByChar(',',L.transforms))
         ),
        'insert_into_table_new', arrayStringConcat(  -- for Reload
            arrayMap(x->
                'insert into ' || splitByChar('#',L.table)[1] || '_new ' || viewcode || '; \n'
            ,splitByChar(',',L.transforms))
         )
     ) as subst
select L.table,
     arrayMap(x->trimBoth(x),splitByChar(',',L.depends_on)) as dep,
     if(delay  = '', 0,   parseTimeDelta(delay))            as delay,
     if(repeat = '', 3600,parseTimeDelta(repeat))           as repeat,
     replaceRegexpAll(replaceRegexpAll(
        arrayStringConcat(arrayMap(x->if(has(mapKeys(subst),x),subst[x],x),splitByRegexp('[\\{\\}]',P.v))),
       '((/\*([^*]|[\r\n]|(\*+([^*/]|[\r\n])))*\*+/)|(--.*))',''),   -- https://blog.ostermiller.org/finding-comments-in-source-code-using-regular-expressions/
       '([\r\n]+)',' ') as sql
from (select * from SCH.Params order by updated_at desc limit 1 by key) as P
join (select * from (select * from SCH.Lineage order by updated_at desc limit 1 by table)
                     where processor in ['Step','Reload','Range','sql']
     ) as L
on 'Template' || L.processor = P.key
;
