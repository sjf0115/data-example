-- kafka 输入表
CREATE TABLE kafka_meta_source_table (
  -- 元数据字段
  `topic` STRING METADATA VIRTUAL, -- 不指定 FROM
  `partition_id` STRING METADATA FROM 'partition' VIRTUAL, -- 指定 FROM
  `offset` BIGINT METADATA VIRTUAL,  -- 不指定 FROM
  `ts` TIMESTAMP(3) METADATA FROM 'timestamp' VIRTUAL, -- 指定 FROM
  -- 业务字段
  `uid` STRING COMMENT '用户Id',
  `wid` STRING COMMENT '微博Id',
  `tm` STRING COMMENT '发微博时间',
  `content` STRING COMMENT '微博内容'
) WITH (
  'connector' = 'kafka',
  'topic' = 'behavior',
  'properties.bootstrap.servers' = 'localhost:9092',
  'properties.group.id' = 'kafka-connector-meta',
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
FROM kafka_meta_source_table;