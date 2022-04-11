package com.flink.example.table.base;/**
 * Created by wy on 2022/4/11.
 */

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * 功能：SQL 版本 WordCount 流处理
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/4/11 下午8:56
 */
public class StreamSQLWordCount {
    public static void main(String[] args) throws Exception {
        // TableEnvironment
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        // 创建输入表
        String sourceSql = "CREATE TABLE source_table (\n" +
                "    word STRING,\n" +
                "    frequency BIGINT\n" +
                ") WITH (\n" +
                "  'connector' = 'datagen',\n" +
                "  'rows-per-second' = '1',\n" +
                "  'fields.word.kind' = 'random',\n" +
                "  'fields.word.length' = '1',\n" +
                "  'fields.frequency.min' = '1',\n" +
                "  'fields.frequency.max' = '9'\n" +
                ")";
        tEnv.executeSql(sourceSql);

        // 创建输出表
        String sinkSql = "CREATE TABLE sink_table (\n" +
                "  word STRING,\n" +
                "  frequency BIGINT\n" +
                ") WITH (\n" +
                "  'connector' = 'print'\n" +
                ")";
        tEnv.executeSql(sinkSql);

        // 聚合查询并输出
        String querySql = "INSERT INTO sink_table\n" +
                "SELECT word, SUM(frequency) AS frequency\n" +
                "FROM source_table\n" +
                "GROUP BY word";
        tEnv.executeSql(querySql);
    }
}
