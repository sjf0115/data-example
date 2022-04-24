package com.flink.example.table.modules;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * 功能：Module SQL 方式示例
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/4/24 下午11:12
 */
public class ModuleSQLExample {
    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);
        // 展示初始加载和启用的模块
        tableEnv.executeSql("SHOW MODULES").print();
        tableEnv.executeSql("SHOW FULL MODULES").print();

        // 加载 Hive 模块
        tableEnv.executeSql("LOAD MODULE hive WITH ('hive-version' = '2.3.7')");
        tableEnv.executeSql("SHOW MODULES").print();
        tableEnv.executeSql("SHOW FULL MODULES").print();

        // 更改模块顺序
        tableEnv.executeSql("USE MODULES hive, core");
        tableEnv.executeSql("SHOW MODULES").print();
        tableEnv.executeSql("SHOW FULL MODULES").print();

        // 禁用 core 模块
        tableEnv.executeSql("USE MODULES hive");
        tableEnv.executeSql("SHOW MODULES").print();
        tableEnv.executeSql("SHOW FULL MODULES").print();

        // 卸载 Hive 模块
        tableEnv.executeSql("UNLOAD MODULE hive");
        tableEnv.executeSql("SHOW MODULES").print();
        tableEnv.executeSql("SHOW FULL MODULES").print();
    }
}
