/*
 find topics ready to be executed. check dependencies and time schedule
 topic format - Fact.Table#tag:shard
 */
CREATE or replace VIEW SCH.Tasks on cluster '{cluster}' as
WITH splitByChar(':', topic)[1] as table_tag,
     toUInt8OrZero(splitByChar(':', topic)[2]) as topic_shard,
  topics as (
      SELECT topic,
             run,
             processor,
             hostid,
             snowflakeToDateTime(last)                                  as last,
             topic_shard,
             dictGet('SCH.LineageDict', 'sql', table_tag)                   AS sql,
             dictGet('SCH.LineageDict', 'repeat', table_tag)                AS repeat,
             dictGet('SCH.LineageDict', 'delay', table_tag)                 AS delay,
             dictGet('SCH.LineageDict', 'time', table_tag)                  AS time,
             arrayJoin(arrayPushFront(dictGet('SCH.LineageDict', 'dependencies', table_tag),
                 dictGet('SCH.LineageDict', 'source', table_tag))) AS dependencies
      FROM SCH.Offsets
      WHERE sql != ''
  ),
  tables as (
      SELECT table_tag as tt, snowflakeToDateTime(last) as last FROM SCH.Offsets WHERE processor != 'deleted'
        union all
      select database||'.'||name,last_successful_update_time last from system.dictionaries where last > 0
        union all
      select database||'.'||name as tt,max(event_time) as last from system.part_log
      where event_time > now() - interval 2 hour
        and event_type='NewPart'
      group by tt
  ),
  updates as (
         SELECT topics.topic                           AS topic,
                any(topics.processor)                  AS processor,
                any(topics.run)                        AS run,
                any(topics.topic_shard)                AS shard,
                any(topics.hostid)                     AS hostid,
                any(topics.repeat)                     AS repeat,
                any(topics.delay)                      AS delay,
                any(topics.time)                       AS time,
                any(topics.sql)                        AS sql,
                any(topics.last)                       AS last,
                minIf(tables.last, tables.last != 0)       AS mins
         FROM topics LEFT JOIN tables ON topics.dependencies = tables.tt
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
      )
  and ( -- time of day limiting
       length(time) = 0
    or Hour(now())>= time[1] and Minute(now()) >= time[2] and Hour(now())<= time[3] and Minute(now()) <= time[4]
    )
;


