/*
 find topics ready to be executed. check dependencies and time schedule
 topic format - Fact.Table:shard#tag
 */
select * from SCH.Tasks;
CREATE or replace VIEW SCH.Tasks on cluster '{cluster}' as
WITH splitByChar(':', topic)[1] as table,
     toUInt8OrZero(splitByChar(':', topic)[2]) as topic_shard,
  O1 as (
      SELECT topic,
             run,
             processor,
             hostid,
             snowflakeToDateTime(last)                                  as last,
             topic_shard,
             dictGet('SCH.LineageDict', 'sql', table)                   AS sql,
             dictGet('SCH.LineageDict', 'repeat', table)                AS repeat,
             dictGet('SCH.LineageDict', 'delay', table)                 AS delay,
             dictGet('SCH.LineageDict', 'time', table)                  AS time,
             arrayJoin(arrayPushFront(dictGet('SCH.LineageDict', 'dependencies', table),
                 dictGet('SCH.LineageDict', 'source', table))) AS dependencies
      FROM SCH.Offsets
      WHERE sql != ''
  ),
  O2 as (
      SELECT table, snowflakeToDateTime(last) as last FROM SCH.Offsets WHERE processor != 'deleted'
        union all
      select database||'.'||name,last_successful_update_time last from system.dictionaries where last > 0
  ),
  updates as (
         SELECT O1.topic                           AS topic,
                any(O1.processor)                  AS processor,
                any(O1.run)                        AS run,
                any(O1.topic_shard)                AS shard,
                any(O1.hostid)                     AS hostid,
                any(O1.repeat)                     AS repeat,
                any(O1.delay)                      AS delay,
                any(O1.time)                       AS time,
                any(O1.sql)                        AS sql,
                any(O1.last)                       AS last,
                minIf(O2.last, O2.last != 0)       AS mins
         FROM O1 LEFT JOIN O2 ON O1.dependencies = O2.table
         GROUP BY topic
    )
SELECT topic,
       if(hosts.host_name != '', hosts.host_name, hostName())          AS hostname,
                                                                          sql,
       least(if(mins = 0, now(), mins), now() - interval delay second) AS upto,
       now()                                                           AS ts,
       toUInt32(now()) - toUInt32(toDateTime('2023-01-01 00:00:00'))   AS seq,
       hostid
FROM  updates
-- for shard processing need a resolvable host_name from system.clusters
LEFT JOIN ( SELECT host_name, shard_num FROM system.clusters WHERE cluster = getMacro('cluster')) AS hosts
ON shard = hosts.shard_num
where (
       processor like 'Full%'
    or last < upto and datediff(second , run, now()) > repeat
    or last < now() - interval 1 day   --??
      )
  and ( -- time of day limiting
       length(time) = 0
    or Hour(now())>= time[1] and Minute(now()) >= time[2] and Hour(now())<= time[3] and Minute(now()) <= time[4]
    )
;


