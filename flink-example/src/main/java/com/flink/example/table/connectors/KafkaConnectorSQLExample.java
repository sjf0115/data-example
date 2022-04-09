package com.flink.example.table.connectors;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * 功能：KafkaConnector SQL 示例
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/4/9 下午11:52
 */
public class KafkaConnectorSQLExample {
    public static void main(String[] args) {
        // TableEnvironment
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .useBlinkPlanner()
                .build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        // 创建 Kafka Source 表
        String sourceSql = "CREATE TABLE kafka_source_table (\n" +
                "  word STRING COMMENT '单词',\n" +
                "  frequency BIGINT COMMENT '次数'\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'word',\n" +
                "  'properties.bootstrap.servers' = 'localhost:9092',\n" +
                "  'properties.group.id' = 'kafka-connector-word-sql',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'json',\n" +
                "  'json.ignore-parse-errors' = 'true',\n" +
                "  'json.fail-on-missing-field' = 'false'\n" +
                ")";
        tableEnv.executeSql(sourceSql);

        // 创建 Print Sink 表
        String sinkSql = "CREATE TABLE print_sink_table (\n" +
                "  word STRING,\n" +
                "  frequency BIGINT\n" +
                ") WITH (\n" +
                "  'connector' = 'print'\n" +
                ")";
        tableEnv.executeSql(sinkSql);

        // 执行计算并输出
        String sql = "INSERT INTO print_sink_table\n" +
                "SELECT word, SUM(frequency) AS frequency\n" +
                "FROM kafka_source_table\n" +
                "GROUP BY word";
        tableEnv.executeSql(sql);
    }
}
