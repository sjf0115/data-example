package com.flink.example.table.table;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

/**
 * 功能：Stream 转换为 Table 示例
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/4/23 下午4:00
 */
public class Stream2TableExample {
    public static void main(String[] args) throws Exception {
        // 创建流和表执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 创建 DataStream
        DataStream<Row> dataStream = env.fromElements(
                Row.of("hello", 1),
                Row.of("word", 4),
                Row.of("hello", 1));

        // 示例1 fromDataStream()
        Table table1 = tEnv.fromDataStream(dataStream).as("word", "count");
        table1.printSchema();
        DataStream<Row> stream1 = tEnv.toChangelogStream(table1);
        stream1.print("R1");

        // 示例2 createTemporaryView()
        tEnv.createTemporaryView("input_table", dataStream, $("word"), $("count"));
        Table table2 = tEnv.from("input_table");
        table2.printSchema();
        DataStream<Row> stream2 = tEnv.toChangelogStream(table2);
        stream2.print("R2");

        // 示例3 fromChangelogStream
        Table table3 = tEnv.fromChangelogStream(dataStream);
        table3.printSchema();
        DataStream<Row> stream3 = tEnv.toChangelogStream(table3);
        stream3.print("R3");

        // 执行
        env.execute();
    }
}
//(
//`word` STRING,
//`count` INT
//)
//(
//`word` STRING,
//`count` INT
//)
//(
//`f0` STRING,
//`f1` INT
//)
//R1:2> +I[hello, 1]
//R2:2> +I[hello, 1]
//R2:3> +I[word, 4]
//R1:4> +I[hello, 1]
//R1:3> +I[word, 4]
//R2:4> +I[hello, 1]
//R3:2> +I[hello, 1]
//R3:3> +I[word, 4]
//R3:4> +I[hello, 1]