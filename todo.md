

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


