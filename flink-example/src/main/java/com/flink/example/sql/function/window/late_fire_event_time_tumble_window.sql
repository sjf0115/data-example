-- 1. 基于事件时间的滚动窗口 迟到触发窗口输出
CREATE TABLE user_behavior (
  uid BIGINT COMMENT '用户Id',
  pid BIGINT COMMENT '商品Id',
  type STRING COMMENT '行为类型',
  `timestamp` BIGINT COMMENT '行为时间',
  ts_ltz AS TO_TIMESTAMP_LTZ(`timestamp`, 3), -- 事件时间
  WATERMARK FOR ts_ltz AS ts_ltz - INTERVAL '5' SECOND -- 在 ts_ltz 上定义watermark，ts_ltz 成为事件时间列
) WITH (
  'connector' = 'kafka',
  'topic' = 'user_behavior',
  'properties.bootstrap.servers' = 'localhost:9092',
  'properties.group.id' = 'user_behavior',
  'scan.startup.mode' = 'latest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'false',
  'json.fail-on-missing-field' = 'true'
)

CREATE TABLE user_behavior_cnt (
  `current_time` STRING COMMENT '当前处理时间',
  window_start STRING COMMENT '窗口开始时间',
  window_end STRING COMMENT '窗口结束时间',
  cnt BIGINT COMMENT '次数'
) WITH (
  'connector' = 'print'
)

INSERT INTO user_behavior_cnt
SELECT
  DATE_FORMAT(now(), 'yyyy-MM-dd HH:mm:ss') AS `current_time`,
  DATE_FORMAT(TUMBLE_START(ts_ltz, INTERVAL '1' MINUTE), 'yyyy-MM-dd HH:mm:ss') AS window_start,
  DATE_FORMAT(TUMBLE_END(ts_ltz, INTERVAL '1' MINUTE), 'yyyy-MM-dd HH:mm:ss') AS window_end,
  COUNT(*) AS cnt
FROM user_behavior
GROUP BY TUMBLE(ts_ltz, INTERVAL '1' MINUTE)