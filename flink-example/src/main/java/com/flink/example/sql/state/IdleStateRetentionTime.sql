--  基于处理时间的滚动窗口 提前触发窗口输出
CREATE TABLE user_behavior (
  uid BIGINT COMMENT '用户Id',
  pid BIGINT COMMENT '商品Id',
  type STRING COMMENT '行为类型',
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
  window_start STRING COMMENT '窗口开始时间',
  window_end STRING COMMENT '窗口结束时间',
  cnt BIGINT COMMENT '次数'
) WITH (
  'connector' = 'print'
)

INSERT INTO user_behavior_cnt
SELECT
  uid, COUNT(*) AS cnt
FROM user_behavior
GROUP BY uid