package com.flink.example.table.table;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * 功能：插入流与表转换简单示例
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/4/10 下午2:40
 */
public class AppendStreamTableSimpleExample {
    public static void main(String[] args) throws Exception {
        // 创建流和表执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 创建 DataStream
        DataStream<String> dataStream = env.fromElements("Alice", "Bob", "John");

        // 将 Insert-Only DataStream 转换为 Table
        Table inputTable = tableEnv.fromDataStream(dataStream);

        // 注册表 InputTable
        tableEnv.createTemporaryView("InputTable", inputTable);

        // 执行查询生成结果 Table
        Table resultTable = tableEnv.sqlQuery("SELECT UPPER(f0) FROM InputTable");

        // 将 Insert-Only Table 转换为 DataStream
        DataStream<Row> resultStream = tableEnv.toDataStream(resultTable);

        // 输出
        resultStream.print();

        // 执行
        env.execute();
    }
}
