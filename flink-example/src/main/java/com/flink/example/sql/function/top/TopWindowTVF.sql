--  基于事件时间的跳跃窗口
CREATE TABLE shop_sales (
  product_id BIGINT COMMENT '商品Id',
  category STRING COMMENT '商品类目',
  price BIGINT COMMENT '行为类型',
  `timestamp` BIGINT COMMENT '行为时间',
  ts_ltz AS TO_TIMESTAMP_LTZ(`timestamp`, 3), -- 事件时间
  WATERMARK FOR ts_ltz AS ts_ltz - INTERVAL '5' SECOND -- 在 ts_ltz 上定义watermark，ts_ltz 成为事件时间列
) WITH (
  'connector' = 'kafka',
  'topic' = 'shop_sales',
  'properties.bootstrap.servers' = 'localhost:9092',
  'properties.group.id' = 'shop_sales',
  'scan.startup.mode' = 'latest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'false',
  'json.fail-on-missing-field' = 'true'
)

CREATE TABLE shop_product_order_top (
  window_start TIMESTAMP(3) COMMENT '窗口开始时间',
  window_end TIMESTAMP(3) COMMENT '窗口结束时间',
  product_id BIGINT COMMENT '商品Id',
  category STRING COMMENT '商品类目',
  price BIGINT COMMENT '订单金额',
  `time` TIMESTAMP_LTZ(3) COMMENT '下单时间',
  row_num BIGINT COMMENT '排名'
) WITH (
  'connector' = 'print'
)

-- 窗口TopN 与窗口 TVF 配合使用
INSERT INTO shop_product_order_top
SELECT
  window_start, window_end,
  product_id, category, price, ts_ltz AS `time`, row_num
FROM (
    SELECT
      window_start, window_end, product_id, category, price, ts_ltz,
      ROW_NUMBER() OVER (PARTITION BY window_start, window_end ORDER BY price DESC) AS row_num
    FROM TABLE(
      TUMBLE(TABLE shop_sales, DESCRIPTOR(ts_ltz), INTERVAL '10' MINUTES)
    )
) WHERE row_num <= 3

CREATE TABLE shop_category_order_top (
  window_start TIMESTAMP(3) COMMENT '窗口开始时间',
  window_end TIMESTAMP(3) COMMENT '窗口结束时间',
  category STRING COMMENT '商品类目',
  price BIGINT COMMENT '订单金额',
  cnt BIGINT COMMENT '订单个数',
  row_num BIGINT COMMENT '排名'
) WITH (
  'connector' = 'print'
)

-- 窗口TopN 与窗口 TVF 聚合函数配合使用
INSERT INTO shop_category_order_top
SELECT
  window_start, window_end, category,
  price, cnt, row_num
FROM (
  SELECT
    window_start, window_end, category,
    price, cnt,
    ROW_NUMBER() OVER (PARTITION BY window_start, window_end ORDER BY price DESC) AS row_num
  FROM (
    SELECT
      window_start, window_end, category,
      SUM(price) AS price, COUNT(*) AS cnt
    FROM TABLE(
      TUMBLE(TABLE shop_sales, DESCRIPTOR(ts_ltz), INTERVAL '10' MINUTES)
    )
    GROUP BY window_start, window_end, category
  )
) WHERE row_num <= 2