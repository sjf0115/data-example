package com.flink.example.table.table;/**
 * Created by wy on 2022/4/10.
 */

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

/**
 * 功能：SQL 方式输出表示例
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/4/10 下午7:59
 */
public class TableOutputExample {
    public static void main(String[] args) throws Exception {
        // 创建流和表执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 创建 DataStream
        DataStream<Row> dataStream = env.fromElements(
                Row.of("Alice", 12),
                Row.of("Bob", 10),
                Row.of("Alice", 100),
                Row.of("Lucy", 50)
        );
        // 将 DataStream 转换为 Table
        Table inputTable = tableEnv.fromDataStream(dataStream).as("name", "score");

        // 1. 通过 Table API executeInsert
        // 聚合计算
        Table outputTable = inputTable
                .filter($("name").isNotEqual("Lucy"))
                .groupBy($("name"))
                .select($("name"), $("score").sum().as("score_sum"));
        // Flink 1.13 版本推荐使用 DDL 方式创建输出表
        tableEnv.executeSql("CREATE TEMPORARY TABLE print_sql_sink (\n" +
                "  name STRING,\n" +
                "  score BIGINT\n" +
                ") WITH (\n" +
                "  'connector' = 'print'\n" +
                ")");
        outputTable.executeInsert("print_sql_sink");

        // 2. 通过 SQL INSERT INTO
        // 注册输入虚拟表
        tableEnv.createTemporaryView("input_table", inputTable);
        tableEnv.executeSql("CREATE TEMPORARY TABLE print_table_sink (\n" +
                "  name STRING,\n" +
                "  score BIGINT\n" +
                ") WITH (\n" +
                "  'connector' = 'print'\n" +
                ")");
        tableEnv.executeSql("INSERT INTO print_table_sink\n" +
                "SELECT name, SUM(score) AS score_sum\n" +
                "FROM input_table\n" +
                "GROUP BY name");
    }
}
