CREATE TABLE behavior_uv (
  behavior STRING COMMENT '行为类型',
  uv BIGINT COMMENT '用户数'
) WITH (
  'connector' = 'print'
)

INSERT INTO behavior_uv
SELECT
  behavior, COUNT(DISTINCT uid) AS uv
FROM user_behavior
GROUP BY behavior