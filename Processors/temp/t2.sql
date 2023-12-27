/*
 todo: Not finished code. Work in progress

 @transform@(step=@step@)

 */

insert into SCH.Params
select 'DAG_BINLOG',$$
set sch_topic = '@topic@',
    max_partitions_per_insert_block=0
;

-- decide what data to process and check for replication lag in source table
insert into SCH.Offsets (topic, next, last, rows,  processor,state,hostid)
    with ( select last from SCH.Offsets where topic='@topic@' ) as _last,
         data as ( select _pos, splitByChar('_', _part) as block
              from @source@
              where _pos > _last and _updated_at < {upto:DateTime}
             ),
    (select count(), max(_pos) from (select _pos from data order by _pos limit @step@)) as stat,
    (select checkBlockSequence(groupUniqArray([toUInt32(block[2]),toUInt32(block[3])])) from data ) as check
    select topic,
        stat.2                                              as next,
        last,
        stat.1                                              as rows,
        if(rows >= @step@,'Full@processor@','@processor@')  as processor,
        if(rows > 0, 'processing', 'delayed' )              as state,
        {HID:String}                                        as hostid
    from SCH.Offsets
    where topic='@topic@'
      and SCH.Offsets.next = 0
      and throwLog(check, 'ERROR','Block Sequence Mismatch. It could be a high replication lag.') = ''
;

-- execute transform view for selected block
create temporary table  __new as (
    with (select last,next from SCH.Offsets where topic='@topic@') as _ln
    select * from (@transform@(step=@step@)) -- where, joins and group by could be here
    where _pos > _ln.1 and _pos <= _ln.2   -- where pushdown to view supposed to work fine for that
)
;
-- store result with additional processing of deleted rows
-- during merges VCMT deletes each pair of rows that have the same primary key and version and different Sign.
-- we can insert duplicates, replacing the same row over and over
insert into @dbtable@
with __old AS ( -- get _version for delete row, if not found - nothing to delete
        select _version,@primary_key@ FROM @dbtable@ final
        PREWHERE (@primary_key@) IN ( SELECT @primary_key@  FROM __new where _sign=-1)
        -- only 1 most fresh, non-delete row
        where sign = 1
        order by _version desc, updated_at desc
        limit 1 by (@primary_key@)
     )
select columns('^[^_].+'),
       if(_sign=-1,__old._version,__new._version) as _version,
       _sign as sign  -- todo: rename to _sign in Fact table
from __new left join __old using (@primary_key@)
where (_sign=1 or _sign=-1 and __old._version != 0)  -- nothing to delete, skip duplicate delete row
-- todo: do not modify same PK by several events in one insert, but it's too complicated and require more testing
-- order by _version desc,_pos desc limit 1 by _orig_pk,_sign
;
$$;

system reload dictionary on cluster '{cluster}' 'SCH.LineageDict' ;
