 Before calling processor code by clickhouse-client (or any other SQL code runner),
  two settings should be set:
 - sch_topic  - id of the object we are going to build
 - log_comment as hostid:runid
    hostid used for mutex exclusion if Scheduler runs on different servers
    runid is only for nice-looking Scheduler logs with correlated lines

 Variables substituted during template processing. 
 Most of them go from SCH.Lineage, possible with some processing by SCH.ProcessTemplates
 
 @topic@        - in format DB.table[#tag][:shard]
 @dbtable@      - dest db.table 
 @table@        - dest table without db
 @source@       - source db.table
 @tag@          - part of topic after # (without shard)
 @step@         - how much data to process on one Transform, could be number of rows of seconds or anything else
 @before@       - any SQL inserted as-is
 @after@        - any SQL inserted as-is
 @transform@    - view code referenced in transform column 
 @primary_key@  - dest table pk from system.tables
 @vcmt_columns@ - dest table columns from system.tables with VCMT replace logic

Run-time variables set during execution:

{upto:DateTime} - max time allowed for processing
