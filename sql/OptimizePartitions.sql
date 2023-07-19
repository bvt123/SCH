--drop dictionary SCH.OptimizePartitions ;
create or replace dictionary SCH.OptimizePartitions on cluster replicated (
    t          String,
    sql        String
) PRIMARY KEY t
layout ( complex_key_direct() )
source (CLICKHOUSE(user 'dict' password 'dict_pass' query '

select t, arrayStringConcat(groupArray(s)) as sql from (
SELECT concat(database, ''.'', table) as t, concat(
    ''insert into SCH.Offsets(topic,last,processor,state) select getSetting(''''agi_topic''''),('''''',toString(max(max_time)),'''''','',toString(max(max_block_number)),''),''''Optimize'''',''''partition ''''||toString('',partition,'');\n'',
    '' optimize table '',database,''.'',table, '' PARTITION '', partition, '' final settings optimize_throw_if_noop=1;\n'',
    ''insert into SCH.Offsets(topic,last,processor) select getSetting(''''agi_topic''''),('''''',toString(max(max_time)),'''''','',toString(max(max_block_number)),''),''''Optimize'''';\n''
    ) AS s
FROM system.parts
WHERE {condition}
  --t in ([''Fact.BetSlip'',''Fact.Transaction''])
  AND active
GROUP BY partition,database,table,t
HAVING count() > 1) group by t
'))
;

create or replace view LagLiveExperimental on cluster replicated
    as;
SELECT topic,
    ifNull(hosts.host_name, hostName()) AS hostname,
    sql,
    now(),
    dateDiff(second, toDateTime('2023-01-01 00:00:00'), now()) as version
FROM (
    WITH splitByChar(':', topic)[1] AS t
    SELECT topic,
        run,
        processor,
        hostid,
        last.1 AS last,
        toUInt8OrZero(splitByChar(':', topic)[2]) AS shard,
        if(      splitByChar( '#', topic )[2] = 'optimize'
            and (dictGet('SCH.LineageDst', 'optimize', t) as hours) != ''
            and toHour(now())*60+toMinute(now()) between toUInt32(splitByChar('-', hours)[1]) and toUInt32(splitByChar('-', hours)[2]) ,
            dictGet('SCH.OptimizePartitions','sql',splitByChar('#', topic)[1]),
            dictGet('SCH.LineageDst', 'sql', t)
        ) AS sql,
        dictGet('SCH.LineageDst', 'repeat', t) AS repeat,
        dictGet('SCH.LineageDst', 'delay', t) AS delay,
        arrayReduce('min', arrayMap(x -> dictGet('SCH.OffsetsDict', 'last', x),
                                    dictGet('SCH.LineageDst', 'depends_on', t))) AS mins
    FROM SCH.Offsets
    WHERE sql != ''
    ) AS updates
     LEFT JOIN ( SELECT host_name, shard_num FROM system.clusters WHERE cluster = 'sharded') AS hosts
    ON shard = hosts.shard_num
WHERE last < (if(mins != 0, mins, now()) - interval delay second)
  AND (dateDiff('second', run, now()) > repeat) OR ((processor = 'FullStep') AND (hostid = ''))
SETTINGS join_use_nulls = 1
;
