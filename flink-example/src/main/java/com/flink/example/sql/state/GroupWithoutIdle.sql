--  空闲状态保留时间
CREATE TABLE order_behavior (
  order_id STRING COMMENT '订单Id',
  uid BIGINT COMMENT '用户Id',
  amount DOUBLE COMMENT '订单金额',
  `timestamp` TIMESTAMP_LTZ(3) COMMENT '下单时间戳',
  WATERMARK FOR `timestamp` AS `timestamp` - INTERVAL '5' SECOND -- 在 ts_ltz 上定义watermark，ts_ltz 成为事件时间列
) WITH (
  'connector' = 'datagen',
  'rows-per-second' = '5000',
  'fields.order_id.kind' = 'random',
  'fields.order_id.length' = '6',
  'fields.uid.kind' = 'random',
  'fields.uid.min' = '10000001',
  'fields.uid.max' = '99999999',
  'fields.amount.kind' = 'random',
  'fields.amount.min' = '1',
  'fields.amount.max' = '1000'
)

CREATE TABLE order_cnt (
  uid BIGINT COMMENT '用户Id',
  cnt BIGINT COMMENT '下单次数'
) WITH (
  'connector' = 'print'
)

INSERT INTO order_cnt
SELECT
  uid, COUNT(*) AS cnt
FROM order_behavior
GROUP BY uid