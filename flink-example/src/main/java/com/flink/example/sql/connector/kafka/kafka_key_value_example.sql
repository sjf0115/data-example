-- 1. kafka 输入表 Key Format 和 Value Format
CREATE TABLE kafka_key_value_source_table (
  uid STRING COMMENT '用户Id',
  wid STRING COMMENT '微博Id',
  tm STRING COMMENT '发微博时间'
) WITH (
  'connector' = 'kafka',
  'topic' = 'behavior',
  'properties.bootstrap.servers' = 'localhost:9092',
  'properties.group.id' = 'kafka-connector-key-value',
  'scan.startup.mode' = 'earliest-offset',
  -- Key Format
  'key.format' = 'json',
  'key.json.ignore-parse-errors' = 'true',
  'key.fields' = 'uid;wid',
  -- Value Format
  'value.format' = 'json',
  'value.json.fail-on-missing-field' = 'false',
  'value.fields-include' = 'EXCEPT_KEY'
);

-- Print 输出表
CREATE TABLE print_table (
  `uid` STRING COMMENT '用户Id',
  `wid` STRING COMMENT '微博Id',
  `tm` STRING COMMENT '发微博时间'
) WITH (
  'connector' = 'print',
  'print-identifier' = 'behavior'
);

SET pipeline.name = 'kafka-connector-key-value';

-- ETL
INSERT INTO print_table
SELECT * FROM kafka_key_value_source_table;

-- Key 格式：
-- {
--   "uid": "fa5aed172c062c61e196eac61038a03b",
--   "wid": "7cce78a4ad39a91ec1f595bcc7fb5eba"
-- }
-- Value 格式：
-- {
--   "tm": "2015-08-01 14:06:31",
--   "content": "卖水果老人"
-- }