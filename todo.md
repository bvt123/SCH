
### Examples
- Offset (init insert)
- Lineage (init insert)
- Stage table from Kafka
- Offset position for Stage table
- Log view

### Habr article
 - russian?

 - alternatives 
   - DBT
   - AirFlow
   - solutions on Kafka connect by clickhouse and altinity 
 - Kafka Engine problems (at-least-once, timing)
 - Kimball theory references 
   - Fact
   - Dimensions as dicts
   - degenerated Dims as LowCardinality
 - why?  
   - it's not possible to make a joins in OLTP when all teams are very busy  
 - getting data from MySQL
   - incremental vs full reload
   - mysql() function
   - Kafka
   - MaxWell and Debezium
 - Stage layer as a Kafka topic (with position)
 - Joins in external loop
 - Dependancies problem
 - last arrival fact problem
 - updates
 - duplicate sources
   - Kafka retries
   - human mistakes

### Maxwell runner
 - as a separate project?
 - Readme.md
 - bash script
 - .my.conf
 - position table create and init insert


###MySQL processor

 - read data from first table in deps list as a mysql(db,table=table)
 - read to temp table
 - store upper position to next
 - if position is set - read data with upper bound
 
### run at time processor
- like unix at/cron
- store last run position and results in Offsets
- any SQL code in processor template

###Scheduler to go

- config with creds
- LagLive request loop 
- async sql runner


