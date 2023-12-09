create or replace function schId as () ->
  bitOr(dateTime64ToSnowflake(now64(3)),
   bitAnd(bitAnd((rowNumberInAllBlocks() as ch),0x3FFFFF)+
      bitAnd(bitShiftRight(ch, 20),0x3FFFFF)+
      bitAnd(bitShiftRight(ch, 40),0x3FFFFF),
      0x3FFFFF)
  );

create or replace function schBlock as (_p) -> (
     _p > ((select last,next from SCH.Offsets where topic=getSetting('sch_topic')) as _ln).1 and _p <= _ln.2
);

create or replace function checkBlockSequence as (arr) ->
-- input: [array of array[start,end]]
-- output: bool
    arraySum(
        (arrayFilter(x,y->y%2=0,
            (arrayPopFront
                (arrayDifference
                    (arrayFlatten(arraySort(arr))
                )
            ) as s),
            arrayEnumerate(s) ) as t
        )
    ) != length(t)
;