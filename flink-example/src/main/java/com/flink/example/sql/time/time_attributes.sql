-- 1. 示例1 事件时间 TIMESTAMP_LTZ
-- 时间戳数据是一个纪元 (epoch) 时间，一般是一个 Long 值，例如 1618989564564，建议事件时间属性使用 TIMESTAMP_LTZ 数据类型的列
CREATE TABLE user_behavior (
  uid BIGINT COMMENT '用户Id',
  pid BIGINT COMMENT '商品Id',
  cid BIGINT COMMENT '商品类目Id',
  type STRING COMMENT '行为类型',
  timestamp BIGINT COMMENT '行为时间', -- 时间戳 Long 型 1618989564564
  ts_ltz AS TO_TIMESTAMP_LTZ(timestamp, 3),
  WATERMARK FOR ts_ltz AS ts_ltz - INTERVAL '1' MINUTE -- 在 ts_ltz 上定义watermark，ts_ltz 成为事件时间列
) WITH (
  'connector' = 'kafka',
  'topic' = 'user_behavior',
  'properties.bootstrap.servers' = 'localhost:9092',
  'properties.group.id' = 'user_behavior_timestamp',
  'scan.startup.mode' = 'latest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true',
  'json.fail-on-missing-field' = 'false'
)

CREATE TABLE user_behavior_detail (
  uid BIGINT COMMENT '用户Id',
  pid BIGINT COMMENT '商品Id',
  cid BIGINT COMMENT '商品类目Id',
  type STRING COMMENT '行为类型',
  timestamp BIGINT COMMENT '行为时间',
  ts_ltz TIMESTAMP_LTZ(3) COMMENT '事件时间属性'
) WITH (
  'connector' = 'print',
  'print-identifier' = 'ET',
  'sink.parallelism' = '1'
)

INSERT INTO user_behavior_detail
SELECT * FROM user_behavior

CREATE TABLE user_behavior_cnt (
  row_time TIMESTAMP(3) COMMENT '窗口开始时间',
  window_start TIMESTAMP(3) COMMENT '窗口开始时间',
  window_end TIMESTAMP(3) COMMENT '窗口结束时间',
  cnt BIGINT COMMENT '次数'
) WITH (
  'connector' = 'print',
  'print-identifier' = 'ET',
  'sink.parallelism' = '1'
)

INSERT INTO user_behavior_cnt
SELECT
  TUMBLE_ROWTIME(ts_ltz, INTERVAL '1' HOUR) AS row_time,
  TUMBLE_START(ts_ltz, INTERVAL '1' HOUR) AS window_start,
  TUMBLE_END(ts_ltz, INTERVAL '1' HOUR) AS window_end,
  COUNT(*) AS cnt
FROM user_behavior
GROUP BY TUMBLE(ts_ltz, INTERVAL '1' HOUR)

-- 2. 示例2 事件时间 TIMESTAMP
-- 没有时区信息的字符串值，例如 2020-04-15 20:13:40.564，建议事件时间属性使用 TIMESTAMP 数据类型的列
CREATE TABLE user_behavior_timestamp (
  uid BIGINT COMMENT '用户Id',
  pid BIGINT COMMENT '商品Id',
  cid BIGINT COMMENT '商品类目Id',
  type STRING COMMENT '行为类型',
  `time` TIMESTAMP(3) COMMENT '行为时间', -- 字符串类型 2020-04-15 20:13:40.564
  WATERMARK FOR `time` AS `time` - INTERVAL '1' MINUTE -- 在 time 上定义watermark，time 成为事件时间列
) WITH (
  'connector' = 'kafka',
  'topic' = 'user_behavior',
  'properties.bootstrap.servers' = 'localhost:9092',
  'properties.group.id' = 'user_behavior_timestamp',
  'scan.startup.mode' = 'latest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'false',
  'json.fail-on-missing-field' = 'true'
)

SELECT TUMBLE_START(`time`, INTERVAL '1' HOUR), COUNT(DISTINCT uid)
FROM user_behavior_timestamp
GROUP BY TUMBLE(`time`, INTERVAL '1' HOUR)

-- 3. 示例3 处理时间
CREATE TABLE user_behavior_process_time (
  uid BIGINT COMMENT '用户Id',
  pid BIGINT COMMENT '商品Id',
  cid BIGINT COMMENT '商品类目Id',
  type STRING COMMENT '行为类型',
  proctime AS PROCTIME() -- 通过计算列产生一个处理时间列
) WITH (
  'connector' = 'kafka',
  'topic' = 'user_behavior',
  'properties.bootstrap.servers' = 'localhost:9092',
  'properties.group.id' = 'user_behavior_process_time',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'false',
  'json.fail-on-missing-field' = 'true'
)

INSERT INTO user_behavior_result
SELECT TUMBLE_START(proctime, INTERVAL '1' HOUR), COUNT(DISTINCT uid)
FROM user_behavior_process_time
GROUP BY TUMBLE(proctime, INTERVAL '1' HOUR)