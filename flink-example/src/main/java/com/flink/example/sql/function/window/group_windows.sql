CREATE TABLE user_behavior (
  uid BIGINT COMMENT '用户Id',
  pid BIGINT COMMENT '商品Id',
  cid BIGINT COMMENT '商品类目Id',
  type STRING COMMENT '行为类型',
  ts TIMESTAMP(3) COMMENT '行为时间',
  proctime AS PROCTIME(), -- 通过计算列产生一个处理时间列
  WATERMARK FOR ts AS ts - INTERVAL '1' MINUTE -- 在 ts 上定义watermark，ts 成为事件时间列
) WITH (
  'connector' = 'kafka',
  'topic' = 'user_behavior',
  'properties.bootstrap.servers' = 'localhost:9092',
  'properties.group.id' = 'kafka-connector-value',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true',
  'json.fail-on-missing-field' = 'true'
);

SELECT
  uid,
  TUMBLE_START(`timestamp`, INTERVAL '1' DAY) AS wStart,
  SUM(amount)
FROM Orders
GROUP BY TUMBLE(order_time, INTERVAL '1' DAY), user;