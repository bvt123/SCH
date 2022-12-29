create or replace view bvt.ProcessTemplates as
with dictGet('bvt.systemViews','create',trimBoth(x)) as viewcode,
     map('table'  , L.table,
        'src'    , dep[1],
        'before' , before,
        'after'  , after,
        'maxstep', maxstep,
        'delay'  , toString(delay),
        'repeat' , toString(repeat),
        'insert_into_table', arrayStringConcat(
            arrayMap(x->
                'insert into ' || L.table || ' ' || viewcode || '; \n'
            ,splitByChar(',',L.transforms))
         ),
        'insert_into_table_new', arrayStringConcat(  -- for Reload
            arrayMap(x->
                'insert into ' || L.table || '_new ' || viewcode || '; \n'
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
from (select * from bvt.Params order by updated_at desc limit 1 by key) as P
join (select * from bvt.Lineage where processor in ['Step','Reload','sql'] order by updated_at desc limit 1 by table) as L
on 'Template' || L.processor = P.key
;
