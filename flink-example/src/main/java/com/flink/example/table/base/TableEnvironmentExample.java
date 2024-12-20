package com.flink.example.table.base;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * 功能：TableEnvironment
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/3/31 下午9:25
 */
public class TableEnvironmentExample {
    public static void main(String[] args) {

        // 1. Streaming & BlinkPlanner
//        EnvironmentSettings settings1 = EnvironmentSettings
//                .newInstance()
//                .useBlinkPlanner()
//                .inStreamingMode()
//                .build();
//
//        TableEnvironment tableEnv1 = TableEnvironment.create(settings1);

        // 2. Streaming & OldPlanner
        EnvironmentSettings settings2 = EnvironmentSettings
                .newInstance()
                .useOldPlanner()
                .inStreamingMode()
                .build();

        TableEnvironment tableEnv2 = TableEnvironment.create(settings2);
//
//        // 3. Batch & BlinkPlanner
//        EnvironmentSettings settings3 = EnvironmentSettings
//                .newInstance()
//                .inBatchMode()
//                .useBlinkPlanner()
//                .build();
//
//        TableEnvironment tableEnv3 = TableEnvironment.create(settings3);
//
//
//        // 4. Batch & OldPlanner
//        EnvironmentSettings settings4 = EnvironmentSettings
//                .newInstance()
//                .inBatchMode()
//                .useOldPlanner()
//                .build();
//
//        TableEnvironment tableEnv4 = TableEnvironment.create(settings4);

        // un(tableEnv1);
        run(tableEnv2);
    }

    /**
     * 运行 SQL
     */
    private static void run(TableEnvironment tableEnv) {
        // 创建输入表
        String sourceSql = "CREATE TABLE kafka_word_table (\n" +
                "  word STRING COMMENT '单词',\n" +
                "  frequency bigint COMMENT '次数'\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'word',\n" +
                "  'properties.bootstrap.servers' = 'localhost:9092',\n" +
                "  'properties.group.id' = 'kafka-connector-word',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'json',\n" +
                "  'json.ignore-parse-errors' = 'true',\n" +
                "  'json.fail-on-missing-field' = 'true'\n" +
                ")";
//        String sourceSql = "CREATE TABLE kafka_word_table (\n" +
//                "  word STRING COMMENT '单词',\n" +
//                "  frequency bigint COMMENT '次数'\n" +
//                ") WITH (\n" +
//                "  'connector.type' = 'kafka',\n" +
//                "  'connector.version' = 'universal',\n" +
//                "  'connector.topic' = 'word',\n" +
//                "  'connector.properties.bootstrap.servers' = 'localhost:9092',\n" +
//                "  'connector.properties.group.id' = 'kafka-connector-word',\n" +
//                "  'connector.startup-mode' = 'earliest-offset',\n" +
//                "  'format.type' = 'json'" +
//                ")";

        tableEnv.executeSql(sourceSql);

        // 创建输出表
        String sinkSql = "CREATE TABLE print_table (\n" +
                "  word STRING,\n" +
                "  frequency INT\n" +
                ") WITH (\n" +
                "  'connector' = 'print'\n" +
                ")";
        tableEnv.executeSql(sinkSql);

        // 执行计算并输出
        String sql = "INSERT INTO print_table\n" +
                "SELECT word, SUM(frequency) AS frequency\n" +
                "FROM kafka_word_table\n" +
                "GROUP BY word";
        tableEnv.executeSql(sql);
    }
}
