/*Key 格式：
{
  -- 格式: <uid>-<wid>
  "uid": "fa5aed172c062c61e196eac61038a03b-7cce78a4ad39a91ec1f595bcc7fb5eba",
}
Value 格式：
{
  "uid": "fa5aed172c062c61e196eac61038a03b",
  "wid": "7cce78a4ad39a91ec1f595bcc7fb5eba",
  "tm": "2015-08-01 14:06:31",
  "content": "卖水果老人"
}*/

-- 1. Kafka 输入表 Key Format 和 Value Format 中有相同字段名称
CREATE TABLE kafka_same_name_source_table (
  `key_uid` STRING COMMENT 'kafka消息的key',
  `uid` STRING COMMENT '用户Id',
  `wid` STRING COMMENT '微博Id',
  `tm` STRING COMMENT '发微博时间'
) WITH (
  'connector' = 'kafka',
  'topic' = 'behavior',
  'properties.bootstrap.servers' = 'localhost:9092',
  'properties.group.id' = 'kafka-connector-same-name',
  'scan.startup.mode' = 'earliest-offset',
  -- Key Format
  'key.format' = 'json',
  'key.fields-prefix' = 'key_',
  'key.fields' = 'key_uid',
  'key.json.ignore-parse-errors' = 'true',
  -- Value Format
  'value.format' = 'json',
  'value.json.ignore-parse-errors' = 'true',
  'value.json.fail-on-missing-field' = 'false',
  'value.fields-include' = 'EXCEPT_KEY' --key 字段不在消息 Value 部分中
);

-- Print 输出表
CREATE TABLE print_table (
  `key_uid` STRING COMMENT 'kafka消息的key',
  `uid` STRING COMMENT '用户Id',
  `wid` STRING COMMENT '微博Id',
  `tm` STRING COMMENT '发微博时间'
) WITH (
  'connector' = 'print',
  'print-identifier' = 'behavior'
);

SET pipeline.name = 'kafka-connector-same-name';

-- ETL
INSERT INTO print_table
SELECT * FROM kafka_same_name_source_table;