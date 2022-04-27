-- 创建 Catalog
CREATE CATALOG my_hive_catalog WITH (
    'type' = 'hive',
    'default-database' = 'hive-database',
    'hive-conf-dir' = '/opt/hive/conf'
);
-- 设置 HiveCatalog 为当前会话的 Catalog
USE CATALOG my_hive_catalog;