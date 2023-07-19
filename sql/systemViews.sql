
CREATE or replace DICTIONARY SCH.systemViews on cluster replicated
(
    `name` String,
    `create` String
)
PRIMARY KEY name
SOURCE(CLICKHOUSE(USER 'dict' PASSWORD 'dict_pass' QUERY '\r\n    select database || \'.\' || table as name,as_select as create from system.tables where engine=\'View\' and {condition}\r\n'))
LAYOUT(COMPLEX_KEY_DIRECT);