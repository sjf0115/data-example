-- kafka 输入表
CREATE TABLE kafka_debezium_meta_source_table (
  -- 元数据字段
  `event_time` TIMESTAMP(3) METADATA FROM 'value.source.timestamp' VIRTUAL,  -- from Debezium format
  `source_table` STRING METADATA FROM 'value.source.table' VIRTUAL, -- from Debezium format
  `topic` STRING METADATA VIRTUAL, -- from Kafka connector
  `partition_id` BIGINT METADATA FROM 'partition' VIRTUAL,  -- from Kafka connector
  `offset` BIGINT METADATA VIRTUAL,  -- from Kafka connector
  -- 业务字段
  `uid` STRING COMMENT '用户Id',
  `wid` STRING COMMENT '微博Id',
  `tm` STRING COMMENT '发微博时间',
  `content` STRING COMMENT '微博内容'
) WITH (
  'connector' = 'kafka',
  'topic' = 'behavior',
  'properties.bootstrap.servers' = 'localhost:9092',
  'properties.group.id' = 'kafka-debezium-meta-example',
  'scan.startup.mode' = 'earliest-offset',
  'value.format' = 'json',
  'value.json.ignore-parse-errors' = 'true'
);

-- Print 输出表
CREATE TABLE print_table (
  `topic` STRING COMMENT 'Kafka 记录的 Topic 名',
  `partition_id` STRING COMMENT 'Kafka 记录的 partition ID',
  `offset` BIGINT COMMENT 'Kafka 记录在 partition 中的 offset',
  `ts` TIMESTAMP(3) COMMENT 'Kafka 记录的时间戳',
  `uid` STRING COMMENT '用户Id',
  `wid` STRING COMMENT '微博Id',
  `tm` STRING COMMENT '发微博时间'
) WITH (
  'connector' = 'print',
  'print-identifier' = 'behavior'
);

-- 设置作业名称
SET pipeline.name = 'kafka-connector-meta';

-- ETL
INSERT INTO print_table
SELECT
  `topic`, `partition_id`, `offset`, `ts`,
  `uid`, `wid`, `tm`
FROM kafka_debezium_meta_source_table;


-- 在当前文件目录执行提交SQL命令: sql-client.sh embedded -f kafka_meta_example.sql