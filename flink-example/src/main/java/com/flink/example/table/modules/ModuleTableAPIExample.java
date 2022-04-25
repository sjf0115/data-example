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

        // 加载 Hive Module
        tableEnv.loadModule("hive", new HiveModule());
        tableEnv.listModules();
        tableEnv.listFullModules();

        // 变更解析顺序
        tableEnv.useModules("hive", "core");
        tableEnv.listModules();
        tableEnv.listFullModules();

        // 禁用 core 模块
        tableEnv.useModules("hive");
        tableEnv.listModules();
        tableEnv.listFullModules();

        // 卸载 Hive 模块
        tableEnv.unloadModule("hive");
        tableEnv.listModules();
        tableEnv.listFullModules();
    }
}
