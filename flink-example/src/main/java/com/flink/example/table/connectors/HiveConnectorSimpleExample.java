package com.flink.example.table.connectors;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

/**
 * 功能：Hive Connector 简单示例
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/4/26 下午10:48
 */
public class HiveConnectorSimpleExample {
    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        String name            = "my_hive_catalog";
        String defaultDatabase = "default";
        String hiveConfDir     = "/opt/hive/conf";

        // 创建 Catalog
        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir);

        // 注册 Catalog
        tEnv.registerCatalog("my-hive", hive);

        // 为当前会话设置 Catalog
        tEnv.useCatalog("my-hive");

        // 列出所有 Catalog
        String[] catalogs = tEnv.listCatalogs();
        for (String catalog : catalogs) {
            System.out.println("Catalog: " + catalog);
        }

        // 列出所有 Hive 表
        String[] tables = tEnv.listTables();
        for (String table : tables) {
            System.out.println("Table: " + table);
        }

        // 查询 Hive 表
        tEnv.executeSql("SELECT * FROM behavior LIMIT 2").print();
    }
}
