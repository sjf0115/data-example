package com.flink.example.table.table;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * 功能：变更流与表转换简单示例
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/4/10 下午2:52
 */
public class ChangeStreamTableSimpleExample {
    public static void main(String[] args) throws Exception {
        // 创建流和表执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 创建 DataStream
        DataStream<Row> dataStream = env.fromElements(
                Row.of("Alice", 12),
                Row.of("Bob", 10),
                Row.of("Alice", 100));

        // 将 Append-Only DataStream 转换为 Table
        Table inputTable = tableEnv.fromDataStream(dataStream).as("name", "score");

        // 注册表 InputTable
        tableEnv.createTemporaryView("InputTable", inputTable);

        // 执行聚合查询生成结果 Table
        Table resultTable = tableEnv.sqlQuery("SELECT name, SUM(score) FROM InputTable GROUP BY name");

        // 将变更 Table 转换为 Changelog DataStream
        DataStream<Row> resultStream = tableEnv.toChangelogStream(resultTable);

        // 输出
        resultStream.print();

        // 执行
        env.execute();
    }
}

//+I[Alice, 12]
//+I[Bob, 10]
//-U[Alice, 12]
//+U[Alice, 112]
