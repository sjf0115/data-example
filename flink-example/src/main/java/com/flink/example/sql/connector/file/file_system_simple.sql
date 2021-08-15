-- FileSystem
CREATE TABLE behavior (
  uid STRING COMMENT '用户Id',
  wid STRING COMMENT '微博Id',
  tm STRING COMMENT '发微博时间',
  content STRING COMMENT '微博内容'
)
PARTITIONED BY (part_name1, part_name2)
WITH (
  'connector' = 'filesystem',           -- 必填项：使用 FileSystem connector
  'path' = 'file:///path/to/whatever',  -- 必填项：指定文件路径
  'format' = 'csv',                     -- 必填项：文件格式
  'partition.default-name' = '...',     -- 可选项：默认分区名称
)

-- Print
CREATE TABLE behavior_print_table (
  uid STRING COMMENT '用户Id',
  wid STRING COMMENT '微博Id',
  tm STRING COMMENT '发微博时间',
  content STRING COMMENT '微博内容'
) WITH (
  'connector' = 'print',
  'print-identifier' = 'behavior',
  'sink.parallelism' = 1
);