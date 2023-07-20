CREATE or replace VIEW daniel.LagLive on cluster replicated
            (
             `topic` LowCardinality(String),
             `hostname` String,
             `sql` String,
             `ts` DateTime,
             `seq` Int64
                )
AS
WITH if(mins != 0, mins, now()) AS mins_now
SELECT topic,
       ifNull(hosts.host_name, hostName())                           AS hostname,
       --format('select \'{0}\'', topic) as sql,
       'select count() from numbers(1e12)'                           AS sql,
       now()                                                         AS ts,
       toUInt32(now()) - toUInt32(toDateTime('2023-01-01 00:00:00')) AS seq
FROM (
         SELECT O1.topic                           AS topic,
                any(O1.processor)                  AS processor,
                any(O1.run)                        AS run,
                any(O1.shard)                      AS shard,
                any(O1.hostid)                     AS hostid,
                any(O1.repeat)                     AS repeat,
                any(O1.delay)                      AS delay,
                any(O1.time)                       AS time,
                any(O1.sql)                        AS sql,
                any(O1.last)                       AS last,
                minIf(O2.last.1, (O2.last.1) != 0) AS mins
         FROM (
                  WITH splitByChar(':', topic)[1] AS t
                  SELECT topic,
                         run,
                         processor,
                         hostid,
                         last.1                                                AS last,
                         toUInt8OrZero(splitByChar(':', topic)[2])             AS shard,
                         dictGet('SCH.LineageDst', 'sql', t)                   AS sql,
                         dictGet('SCH.LineageDst', 'repeat', t)                AS repeat,
                         dictGet('SCH.LineageDst', 'delay', t)                 AS delay,
                         dictGet('SCH.LineageDst', 'time', t)                  AS time,
                         arrayJoin(dictGet('SCH.LineageDst', 'depends_on', t)) AS dep
                  FROM SCH.Offsets
                  WHERE sql != ''
                  ) AS O1
                  LEFT JOIN
              (
                  SELECT *,
                         splitByChar(':', topic)[1] AS t
                  FROM (
                           SELECT *
                           FROM SCH.Offsets
                           UNION ALL
                           SELECT *
                           FROM SCH.OffsetsLocal
                           )
                  WHERE processor != 'deleted'
                  ORDER BY last DESC
                  LIMIT 1 BY topic
                  ) AS O2 ON O1.dep = O2.t
         GROUP BY topic
         ) AS updates
         LEFT JOIN
     (
         SELECT host_name,
                shard_num
         FROM system.clusters
         WHERE cluster = 'sharded'
         ) AS hosts ON shard = hosts.shard_num
where topic like '%#%'
    SETTINGS join_use_nulls = 1;

