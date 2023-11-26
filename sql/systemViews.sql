
CREATE or replace DICTIONARY SCH.systemViews on cluster '{cluster}'
(
    `name` String,
    `create` String
)
PRIMARY KEY name
SOURCE(CLICKHOUSE(QUERY '\r\n    select database || \'.\' || table as name,as_select as create from system.tables where engine=\'View\' and {condition}\r\n'))
LAYOUT(COMPLEX_KEY_DIRECT);