/*
__new temp table created at first step to define processed block
@transform@(step=@step@)
no looking back to store table, use replace data from incoming events. suitable only for exactly-once-delivery

 */

insert into SCH.Params
select 'DAG_Version',$$
set sch_topic = '@topic@',
    keeper_map_strict_mode=1,
    max_partitions_per_insert_block=0
;
-- load transformed data block
create temporary table  __new as (
    select * from (
        with (select last,next from SCH.Offsets where topic='@topic@') as _ln
        select * except ( _part ), _pos, splitByChar('_', _part) as _block
        from (@transform@(step=@step@))   -- join and group by could be here
        where _pos > _ln.1                -- where push down should work
          and if(_ln.2 != 0, _pos <= _ln.2, snowflakeToDateTime(_pos) < {upto:DateTime})
        --order by _pos limit @step@  -- should be inside source view
    )
    order by _version desc,_pos desc limit 1 by _orig_pk
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

-- copy data from temp table to dest table with deduplication on insert
insert into @dbtable@
select columns('^[^_].+'),
      _version,
      _sign as sing  -- todo: rename to _sign in Fact table
from __new where
          (@primary_key@,_version,_sign) not in
   (select @primary_key@,_version,sign   from @dbtable@
    where (@primary_key@,_version,sign)  in
   (select @primary_key@,_version,_sign  from __new))
;
$$;

system reload dictionary on cluster '{cluster}' 'SCH.LineageDict' ;
