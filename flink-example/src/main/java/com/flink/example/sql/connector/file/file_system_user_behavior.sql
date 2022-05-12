CREATE TABLE user_behavior (
  uid BIGINT COMMENT '用户Id',
  pid BIGINT COMMENT '商品Id',
  cid BIGINT COMMENT '商品类目Id',
  `type` STRING COMMENT '行为类型',
  ts BIGINT COMMENT '行为时间戳',
  tm STRING COMMENT '行为时间',
  process_time AS PROCTIME() -- 处理时间
)
WITH (
  'connector' = 'filesystem',
  'path' = 'file:///opt/data/user_behavior.csv',
  'format' = 'csv'
)

CREATE TABLE user_behavior (
  uid BIGINT COMMENT '用户Id',
  pid BIGINT COMMENT '商品Id',
  cid BIGINT COMMENT '商品类目Id',
  `type` STRING COMMENT '行为类型',
  ts BIGINT COMMENT '行为时间戳',
  ts BIGINT COMMENT '行为时间戳',
  ts_ltz AS TO_TIMESTAMP_LTZ(ts, 3), -- 事件时间
  WATERMARK FOR ts_ltz AS ts_ltz - INTERVAL '1' MINUTE
)
WITH (
  'connector' = 'filesystem',
  'path' = 'file:///opt/data/user_behavior.csv',
  'format' = 'csv',
  'csv.ignore-parse-errors' = 'true'
)

CREATE TABLE print_sink_table (
  uid BIGINT COMMENT '用户Id',
  cnt BIGINT COMMENT '次数'
) WITH (
  'connector' = 'print',
  'print-identifier' = 'behavior'
);

INSERT INTO print_sink_table
SELECT uid, COUNT(*) as cnt
FROM user_behavior
GROUP BY uid;
