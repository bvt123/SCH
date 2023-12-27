-- reset Offsets to particular time
insert into SCH.Offsets(topic,last,next)
select 'Fact.Example',dateTime64ToSnowflake(toDateTime64('2023-12-15 00:00:00.000',3)),0;
