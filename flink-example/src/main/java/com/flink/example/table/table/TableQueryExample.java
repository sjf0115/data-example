package com.flink.example.table.table;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

/**
 * 功能：Table API & SQL 查询表简单示例
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/4/10 下午4:04
 */
public class TableQueryExample {
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

        // 1. SQL 方式查询表
        // 1.1 通过注册虚拟表
        tableEnv.createTemporaryView("input_table", inputTable);
        // 执行聚合计算
        Table resultTable1 = tableEnv.sqlQuery("SELECT name, SUM(score) AS score_sum\n" +
                "FROM input_table\n" +
                "WHERE name <> 'Lucy'\n" +
                "GROUP BY name");
        // Table 转 Changelog DataStream
        DataStream<Row> resultStream1 = tableEnv.toChangelogStream(resultTable1);
        // 输出
        resultStream1.print("R1");

        // 1.2 通过字符串拼接
        Table resultTable2 = tableEnv.sqlQuery("SELECT name, SUM(score) AS score_sum\n" +
                "FROM " + inputTable +
                " WHERE name <> 'Lucy'\n" +
                "GROUP BY name");
        // Table 转 Changelog DataStream
        DataStream<Row> resultStream2 = tableEnv.toChangelogStream(resultTable2);
        // 输出
        resultStream2.print("R2");

        // 2. Table API 方式查询表
        // 执行聚合计算
        Table resultTable3 = inputTable
                .filter($("name").isNotEqual("Lucy"))
                .groupBy($("name"))
                .select($("name"), $("score").sum().as("score_sum"));
        // Table 转 Changelog DataStream
        DataStream<Row> resultStream3 = tableEnv.toChangelogStream(resultTable3);
        // 输出
        resultStream3.print("R3");

        // 执行
        env.execute();
    }
}
//R2:3> +I[Alice, 12]
//R3:3> +I[Alice, 12]
//R1:3> +I[Alice, 12]
//R2:3> +I[Bob, 10]
//R3:3> +I[Bob, 10]
//R1:3> +I[Bob, 10]
//R2:3> -U[Alice, 12]
//R1:3> -U[Alice, 12]
//R1:3> +U[Alice, 112]
//R2:3> +U[Alice, 112]
//R3:3> -U[Alice, 12]
//R3:3> +U[Alice, 112]