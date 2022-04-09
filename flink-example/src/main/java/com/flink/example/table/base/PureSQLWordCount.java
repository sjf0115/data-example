package com.flink.example.table.base;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * 功能：纯 SQL WordCount
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/4/4 下午9:14
 */
public class PureSQLWordCount {
    public static void main(String[] args) {
        // TableEnvironment
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .useBlinkPlanner()
                .build();

        TableEnvironment tableEnv = TableEnvironment.create(settings);

        // 创建输入表
        String sourceSql = "CREATE TABLE datagen_table (\n" +
                "    word STRING,\n" +
                "    frequency int\n" +
                ") WITH (\n" +
                "  'connector' = 'datagen',\n" +
                "  'rows-per-second' = '1',\n" +
                "  'fields.word.kind' = 'random',\n" +
                "  'fields.word.length' = '1',\n" +
                "  'fields.frequency.min' = '1',\n" +
                "  'fields.frequency.max' = '9'\n" +
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
                "FROM datagen_table\n" +
                "GROUP BY word";
        tableEnv.executeSql(sql);

//        // 执行单词求和
//        String groupSql = "SELECT word, SUM(frequency) AS frequency\n" +
//                "FROM datagen_table\n" +
//                "GROUP BY word";
//        Table resultTable = tableEnv.sqlQuery(groupSql);
//
//        // 输出结果
//        resultTable.executeInsert("print_table");
    }
}
