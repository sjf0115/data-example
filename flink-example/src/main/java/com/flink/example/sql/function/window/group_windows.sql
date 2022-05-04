CREATE TABLE user_behavior (
  uid BIGINT COMMENT '用户Id',
  pid BIGINT COMMENT '商品Id',
  cid BIGINT COMMENT '商品类目Id',
  type STRING COMMENT '行为类型',
  ts BIGINT COMMENT '行为时间',
  ts_ltz AS TO_TIMESTAMP_LTZ(ts, 3),
  proctime AS PROCTIME(), -- 通过计算列产生一个处理时间列
  WATERMARK FOR ts_ltz AS ts_ltz - INTERVAL '1' MINUTE -- 在 ts_ltz 上定义watermark，ts_ltz 成为事件时间列
) WITH (
  'connector' = 'kafka',
  'topic' = 'user_behavior',
  'properties.bootstrap.servers' = 'localhost:9092',
  'properties.group.id' = 'kafka-connector-value',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true',
  'json.fail-on-missing-field' = 'false'
);

-- Sink
CREATE TABLE user_behavior_result (
  uid BIGINT COMMENT '用户Id',
  window_tart TIMESTAMP(3) COMMENT '窗口开始时间',
  window_end TIMESTAMP(3) COMMENT '窗口结束时间',
  num BIGINT COMMENT '条数'
) WITH (
  'connector' = 'print',
  'print-identifier' = 'behavior',
  'sink.parallelism' = '1'
);

INSERT INTO user_behavior_result
SELECT
  uid,
  TUMBLE_START(ts, INTERVAL '1' MINUTE) AS window_tart,
  TUMBLE_END(ts, INTERVAL '1' MINUTE) AS window_end,
  COUNT(*) AS num
FROM user_behavior
GROUP BY TUMBLE(ts, INTERVAL '1' MINUTE), uid;