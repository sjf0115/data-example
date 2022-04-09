package com.flink.example.table.base;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 功能：StreamTableEnvironment 示例
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/4/5 下午7:00
 */
public class StreamTableEnvironmentExample {
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 1. Streaming & BlinkPlanner
//        EnvironmentSettings settings1 = EnvironmentSettings
//                .newInstance()
//                .useBlinkPlanner()
//                .inStreamingMode()
//                .build();
//        StreamTableEnvironment tabEnv1 = StreamTableEnvironment.create(env, settings1);

        // 2. Streaming & OldPlanner
        EnvironmentSettings settings2 = EnvironmentSettings
                .newInstance()
                .useOldPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tabEnv2 = StreamTableEnvironment.create(env, settings2);

        run(tabEnv2);
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

        sourceSql = "CREATE TABLE kafka_word_table (\n" +
                "  word STRING COMMENT '单词',\n" +
                "  frequency bigint COMMENT '次数'\n" +
                ") WITH (\n" +
                "  'connector.type' = 'kafka',\n" +
                "  'connector.version' = 'universal',\n" +
                "  'connector.topic' = 'word',\n" +
                "  'connector.properties.bootstrap.servers' = 'localhost:9092',\n" +
                "  'connector.properties.group.id' = 'kafka-connector-word',\n" +
                "  'connector.startup-mode' = 'earliest-offset',\n" +
                "  'format.type' = 'json'\n" +
                ")";

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
