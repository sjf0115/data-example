package com.flink.example.table.base;

import com.common.example.bean.WordCount;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * 功能：Table 版本 WordCount: Streaming
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/4/10 上午11:41
 */
public class SimpleStreamTableWordCount {
    public static void main(String[] args) throws Exception {
        // 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 读取数据创建 DataStream
        DataStream<WordCount> inputStream = env.fromElements(
                new WordCount("Hello", 1L),
                new WordCount("Ciao", 1L),
                new WordCount("Hello", 1L));

        // DataStream 转 Table
        Table table = tEnv.fromDataStream(inputStream);
        //Table table = tEnv.fromDataStream(inputStream, $("word"), $("frequency"));

        // 执行查询
        Table resultTable = table
                .groupBy($("word"))
                .select($("word"), $("frequency").sum().as("frequency"))
                .as("word, frequency");

        // Table 转 DataStream
        DataStream<Tuple2<Boolean, WordCount>> result = tEnv.toRetractStream(resultTable, WordCount.class);
        result.print();

        // 执行
        env.execute();
    }
}
