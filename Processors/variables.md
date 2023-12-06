 Before calling processor code by clickhouse-client (or any other SQL code runner),
  two settings should be set:
 - sch_topic  - id of the object we are going to build
 - log_comment as hostid:runid
    hostid used for mutex exclusion if Scheduler runs on different servers
    runid is only for nice-looking Scheduler logs with correlated lines

 Variables substituted during template processing:
 
 @topic@   - in format DB.table#xxx .....
 @table@   - dest table
 @source@  - source table
 @tag@     - part of topic after #
 @step@ - how much data to process on one Transform, could be number of rows of seconds or anything else
 @before@                            -- before SQL
 @after@                             -- after SQL

Run-time variables set during execution:

{upto:DateTime} - max time allowed for processing
