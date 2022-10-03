--  基于处理时间的滚动窗口
CREATE TABLE user_behavior (
  uid BIGINT COMMENT '用户Id',
  pid BIGINT COMMENT '商品Id',
  cid BIGINT COMMENT '商品类目Id',
  type STRING COMMENT '行为类型',
  `timestamp` BIGINT COMMENT '行为时间',
  `time` STRING COMMENT '行为时间',
  process_time AS PROCTIME() -- 处理时间
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
  window_start TIMESTAMP(3) COMMENT '窗口开始时间',
  window_end TIMESTAMP(3) COMMENT '窗口结束时间',
  cnt BIGINT COMMENT '次数',
  min_time STRING COMMENT '最小行为时间',
  max_time STRING COMMENT '最大行为时间',
  pid_set MULTISET<BIGINT> COMMENT '商品集合'
) WITH (
  'connector' = 'print'
)

INSERT INTO user_behavior_cnt
SELECT
  window_start, window_end,
  COUNT(*) AS cnt,
  MIN(`time`) AS min_time,
  MAX(`time`) AS max_time,
  COLLECT(DISTINCT pid) AS pid_set
FROM TABLE(
    HOP(TABLE user_behavior, DESCRIPTOR(process_time), INTERVAL '30' SECONDS, INTERVAL '1' MINUTES)
)
GROUP BY window_start, window_end