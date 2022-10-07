-- 1. Datagen Connector 随机生成
CREATE TABLE order_behavior (
  order_id STRING COMMENT '订单Id',
  uid BIGINT COMMENT '用户Id',
  amount DOUBLE COMMENT '订单金额',
  `timestamp` TIMESTAMP_LTZ(3) COMMENT '下单时间戳',
  WATERMARK FOR `timestamp` AS `timestamp` - INTERVAL '5' SECOND -- 在 ts_ltz 上定义watermark，ts_ltz 成为事件时间列
) WITH (
  'connector' = 'datagen',
  'rows-per-second' = '1',
  'fields.order_id.kind' = 'random',
  'fields.order_id.length' = '6',
  'fields.uid.kind' = 'random',
  'fields.uid.min' = '10000001',
  'fields.uid.max' = '99999999',
  'fields.amount.kind' = 'random',
  'fields.amount.min' = '1',
  'fields.amount.max' = '1000'
)

CREATE TABLE order_behavior_print (
  order_id STRING COMMENT '订单Id',
  uid BIGINT COMMENT '用户Id',
  amount DOUBLE COMMENT '订单金额',
  `timestamp` TIMESTAMP_LTZ(3) COMMENT '下单时间戳',
  `time` STRING COMMENT '下单时间'
) WITH (
  'connector' = 'print'
)

INSERT INTO order_behavior_print
SELECT
  order_id, uid, amount, `timestamp`,
  DATE_FORMAT(`timestamp`, 'yyyy-MM-dd HH:mm:ss') AS `time`
FROM order_behavior
