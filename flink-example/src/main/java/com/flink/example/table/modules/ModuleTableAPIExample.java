package com.flink.example.table.modules;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.module.hive.HiveModule;

/**
 * 功能：Module Table API 示例
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/4/24 下午11:28
 */
public class ModuleTableAPIExample {
    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        tableEnv.listModules();
        tableEnv.listFullModules();

        // Load a hive module
        tableEnv.loadModule("hive", new HiveModule());

        // Show all enabled modules
        tableEnv.listModules();
    }
}
