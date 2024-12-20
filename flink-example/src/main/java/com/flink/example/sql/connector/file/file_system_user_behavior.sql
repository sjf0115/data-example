CREATE TABLE user_behavior (
  uid BIGINT COMMENT '用户Id',
  pid BIGINT COMMENT '商品Id',
  cid BIGINT COMMENT '商品类目Id',
  `type` STRING COMMENT '行为类型',
  timestamp BIGINT COMMENT '行为时间戳',
  tm STRING COMMENT '行为时间',
  process_time AS PROCTIME() -- 处理时间
)
WITH (
  'connector' = 'filesystem',
  'path' = 'file:///opt/data/user_behavior.csv',
  'format' = 'csv'
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
