--  基于处理时间的滑动窗口
CREATE TABLE shop_sales (
  product_id BIGINT COMMENT '商品Id',
  category_id BIGINT COMMENT '商品类目Id',
  sales BIGINT COMMENT '下单量',
  process_time AS PROCTIME() -- 处理时间
) WITH (
  'connector' = 'kafka',
  'topic' = 'shop_sales',
  'properties.bootstrap.servers' = 'localhost:9092',
  'properties.group.id' = 'user_behavior',
  'scan.startup.mode' = 'latest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'false',
  'json.fail-on-missing-field' = 'true'
)

CREATE TABLE shop_sales_top (
  category_id BIGINT COMMENT '商品类目Id',
  product_id BIGINT COMMENT '商品Id',
  sales BIGINT COMMENT '下单量',
  row_num BIGINT COMMENT '排名'
) WITH (
  'connector' = 'print'
)

-- Top5
INSERT INTO shop_sales_top
SELECT
  category_id, product_id, sales, row_num
FROM (
  SELECT
    category_id, product_id, sales,
    ROW_NUMBER() OVER (PARTITION BY category_id ORDER BY sales DESC) AS row_num
  FROM shop_sales
)
WHERE row_num <= 5