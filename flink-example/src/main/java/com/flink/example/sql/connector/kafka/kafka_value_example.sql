-- 1. kafka 输入表 只有 Value Format
CREATE TABLE kafka_value_source_table (
  uid STRING COMMENT '用户Id',
  wid STRING COMMENT '微博Id',
  tm STRING COMMENT '发微博时间'
) WITH (
  'connector' = 'kafka',
  'topic' = 'behavior',
  'properties.bootstrap.servers' = 'localhost:9092',
  'properties.group.id' = 'kafka-connector-value',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true',
  'json.fail-on-missing-field' = 'true'
);

-- print 输出表
CREATE TABLE print_table (
  `uid` STRING COMMENT '用户Id',
  `wid` STRING COMMENT '微博Id',
  `tm` STRING COMMENT '发微博时间'
) WITH (
  'connector' = 'print',
  'print-identifier' = 'behavior'
);

SET pipeline.name = 'kafka-connector-value';

-- ETL
INSERT INTO print_table
SELECT * FROM kafka_value_source_table;