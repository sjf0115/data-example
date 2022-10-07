-- 1. Datagen Connector 序列生成器
CREATE TABLE order_behavior (
  order_id BIGINT COMMENT '订单Id'
) WITH (
  'connector' = 'datagen',
  'rows-per-second' = '1',
  'fields.order_id.kind' = 'sequence',
  'fields.order_id.start' = '10000001',
  'fields.order_id.end' = '10000010'
)

CREATE TABLE order_behavior_print (
  order_id BIGINT COMMENT '订单Id'
) WITH (
  'connector' = 'print'
)

INSERT INTO order_behavior_print
SELECT
  order_id
FROM order_behavior