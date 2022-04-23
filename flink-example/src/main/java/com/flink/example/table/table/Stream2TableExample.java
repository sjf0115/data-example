package com.flink.example.table.table;

import com.common.example.bean.WordCount;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * 功能：Stream 转换 Table 示例
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

        // 示例1 fromDataStream()
        DataStream<WordCount> sourceStream1 = env.fromElements(
                new WordCount("hello", 1),
                new WordCount("word", 4),
                new WordCount("hello", 1));
        //Table table1 = tEnv.fromDataStream(sourceStream1, $("word"), $("frequency"));
        Table table1 = tEnv.fromDataStream(sourceStream1);
        table1.printSchema();
        DataStream<WordCount> resultStream1 = tEnv.toAppendStream(table1, WordCount.class);
        resultStream1.print("R1");

        // 示例2 createTemporaryView()
        DataStream<WordCount> sourceStream2 = env.fromElements(
                new WordCount("hello", 1),
                new WordCount("word", 4),
                new WordCount("hello", 1));
        tEnv.createTemporaryView("input_table", sourceStream2);
        //tEnv.createTemporaryView("input_table", sourceStream2, $("word"), $("frequency"));
        Table table2 = tEnv.from("input_table");
        table2.printSchema();
        DataStream<WordCount> resultStream2 = tEnv.toAppendStream(table2, WordCount.class);
        resultStream2.print("R2");

        // 示例3 fromChangelogStream
        DataStream<Row> sourceStream3 = env.fromElements(
                Row.of("hello", 1),
                Row.of("word", 4),
                Row.of("hello", 1));
        Table table3 = tEnv.fromChangelogStream(sourceStream3);
        table3.printSchema();
        DataStream<Row> resultStream3 = tEnv.toChangelogStream(table3);
        resultStream3.print("R3");

        // 执行
        env.execute();
    }
}
//(
//`word` STRING,
//`frequency` BIGINT NOT NULL
//)
//(
//`word` STRING,
//`frequency` BIGINT
//)
//(
//`f0` STRING,
//`f1` INT
//)
//R1:1> WordCount{word='hello', frequency=1}
//R1:3> WordCount{word='hello', frequency=1}
//R1:4> WordCount{word='word', frequency=4}
//R2:1> WordCount{word='word', frequency=4}
//R3:2> +I[hello, 1]
//R2:4> WordCount{word='hello', frequency=1}
//R3:4> +I[hello, 1]
//R3:3> +I[word, 4]
//R2:2> WordCount{word='hello', frequency=1}