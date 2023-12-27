/*
 todo: Not finished code. Work in progress

__new temp table created at first step to define processed block
use json_columns macro
 */

insert into SCH.Params
select 'DAG_JSON',$$
set sch_topic = '@topic@',
    keeper_map_strict_mode=1,
    max_partitions_per_insert_block=0
;

-- load transformed data block
create temporary table  __new as (
    select @json_columns@,_sign,_version,_pos,_block from (
        with (select last,next from SCH.Offsets where topic='@topic@') as _ln
        select untuple(if(op='d',before,after)) as d,
                if(op='d',-1,1)         as _sign,
                ts_ms                   as _version,
                _pos,
                splitByChar('_', _part) as _block
        from @source@
        where _pos > _ln.1                -- where push down should work
          and if(_ln.2 != 0, _pos <= _ln.2, snowflakeToDateTime(_pos) < {upto:DateTime})
        order by _pos limit @step@
    )
    --order by _version desc,_pos desc limit 1 by @primary_key@
)
;
-- save position
insert into SCH.Offsets (topic, next, last, rows,  processor,state,hostid)
    with (select count(), max(_pos) from __new) as stat
    select topic, stat.2, last,
        stat.1                                        as rows,
        if(rows >= 0.5 * @step@,'Full@processor@','@processor@')    as processor,
        if(rows > 0, 'processing', 'delayed' )        as state,
        {HID:String}                                  as hostid
    from SCH.Offsets
    where topic='@topic@'
      and next = 0
settings keeper_map_strict_mode=0;
-- copy data from temp table to dest table with back ref and sign = 1/-1
insert into @dbtable@
with __old AS (
         SELECT *, arrayJoin([-1,1]) AS _sign from (
            select * FROM @dbtable@ final
            PREWHERE (@primary_key@) IN ( SELECT @primary_key@  FROM __new )
            -- some efforts to deal with wrong data in dbtable
            where sign = 1
            order by _version desc, updated_at desc
            limit 1 by (@primary_key@)
        )
     )
select @vcmt_columns@ ,
     __new._version                                  AS _version,
     if(__old._sign = -1, -1, 1)                     AS sign
from __new left join __old using (@primary_key@)
where if(__new._sign = -1, --__new.is_deleted,
  __old._sign = -1,                -- insert only delete row if it's found in old data
  __new._version > __old._version  -- skip duplicates for updates
)
;
$$;

system reload dictionary on cluster '{cluster}' 'SCH.LineageDict' ;
