package com.flink.example.table.table;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * 功能：SQL 表操作简单示例
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/4/10 下午8:08
 */
public class SimpleSQLExample {
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

        // 输入表: 注册虚拟表
        tableEnv.createTemporaryView("input_table", inputTable);

        // 输出表: 创建 Print Connector 表
        tableEnv.executeSql("CREATE TEMPORARY TABLE print_sink_table (\n" +
                "  name STRING,\n" +
                "  score BIGINT\n" +
                ") WITH (\n" +
                "  'connector' = 'print'\n" +
                ")");

        // 聚合计算并输出
        tableEnv.executeSql("INSERT INTO print_sink_table\n" +
                "SELECT name, SUM(score) AS score_sum\n" +
                "FROM input_table\n" +
                "GROUP BY name");

        /*tableEnv.executeSql("INSERT INTO print_sink_table\n" +
                "SELECT name, SUM(score) AS score_sum\n" +
                "FROM " + inputTable +
                " WHERE name <> 'Lucy'\n" +
                "GROUP BY name");*/
    }
}
//3> +I[Alice, 12]
//3> +I[Bob, 10]
//2> +I[Lucy, 50]
//3> -U[Alice, 12]
//3> +U[Alice, 112]