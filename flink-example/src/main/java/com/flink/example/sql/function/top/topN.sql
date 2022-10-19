--  基于处理时间的滑动窗口
CREATE TABLE shop_sales (
  product_id BIGINT COMMENT '商品Id',
  category STRING COMMENT '商品类目',
  price STRING COMMENT '行为类型',
  `timestamp` BIGINT COMMENT '行为时间',
  ts_ltz AS TO_TIMESTAMP_LTZ(`timestamp`, 3), -- 事件时间
  WATERMARK FOR ts_ltz AS ts_ltz - INTERVAL '5' SECOND -- 在 ts_ltz 上定义watermark，ts_ltz 成为事件时间列
) WITH (
  'connector' = 'kafka',
  'topic' = 'user_behavior',
  'properties.bootstrap.servers' = 'localhost:9092',
  'properties.group.id' = 'shop_sales',
  'scan.startup.mode' = 'latest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'false',
  'json.fail-on-missing-field' = 'true'
)

CREATE TABLE shop_category_order_top (
  category_id BIGINT COMMENT '商品类目Id',
  product_id BIGINT COMMENT '商品Id',
  price BIGINT COMMENT '下单量',
  time TIMESTAMP(3) COMMENT '下单时间',
  row_num BIGINT COMMENT '排名'
) WITH (
  'connector' = 'print'
)

-- 每个类目下的下单金额Top3
INSERT INTO shop_category_order_top
SELECT
  category_id, product_id, price, ts_ltz, row_num
FROM (
  SELECT
    category_id, product_id, price, ts_ltz,
    ROW_NUMBER() OVER (PARTITION BY category_id ORDER BY price DESC) AS row_num
  FROM shop_sales
)
WHERE row_num <= 5