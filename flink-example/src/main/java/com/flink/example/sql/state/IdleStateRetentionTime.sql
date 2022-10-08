--  空闲状态保留时间
CREATE TABLE session_behavior (
  session_id STRING COMMENT '会话Id',
  uid BIGINT COMMENT '用户Id'
) WITH (
  'connector' = 'datagen',
  'rows-per-second' = '10',
  'fields.session_id.kind' = 'sequence',
  'fields.session_id.start' = '10001',
  'fields.session_id.end' = '90000',
  'fields.uid.kind' = 'random',
  'fields.uid.min' = '1001',
  'fields.uid.max' = '9999'
)

CREATE TABLE session_cnt (
  session_id STRING COMMENT '会话Id',
  uid BIGINT COMMENT '用户Id'
) WITH (
  'connector' = 'blackhole'
)

INSERT INTO session_cnt
SELECT
  session_id, COUNT(*) AS cnt
FROM session_behavior
GROUP BY session_id