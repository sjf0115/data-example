package com.flink.example.stream.sink.jdbc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * 功能：通过 JDBC Sink 输出到 MySQL
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2024/8/25 18:18
 */
public class JdbcMysqlExample {
    public static void main(String[] args) throws Exception {
        String hostname = "localhost";
        int port = 9100;
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 单词流
        DataStream<String> text = env.socketTextStream(hostname, port, "\n");
        DataStream<Tuple2<String, Integer>> words = text.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] words = line.split("\\s+");
                for (String word : words) {
                    collector.collect(Tuple2.of(word, 1));
                }
            }
        });

        // 单词计数
        DataStream<Tuple2<String, Integer>> wordsCount = words.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> tuple) throws Exception {
                return tuple.f0;
            }
        }).reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> a, Tuple2<String, Integer> b) throws Exception {
                return new Tuple2(a.f0, a.f1 + b.f1);
            }
        });

        // 输出到控制台
        wordsCount.print();

        // 输出到 MySQL
        wordsCount.addSink(JdbcSink.sink(
                "INSERT INTO word_count_upsert (word, count) VALUES (?, ?) ON DUPLICATE KEY UPDATE count = ?",
                //"insert into word_count_append (word, count) values (?, ?)",
                (statement, tuple2) -> {
                    statement.setString(1, tuple2.f0);
                    statement.setLong(2, tuple2.f1);
                    statement.setLong(3, tuple2.f1);
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(1)
                        .withBatchIntervalMs(200)
                        .withMaxRetries(5)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://localhost:3308/flink")
                        .withDriverName("com.mysql.cj.jdbc.Driver")
                        .withUsername("root")
                        .withPassword("root")
                        .build()
        ));

        env.execute("JdbcMysqlExample");
    }
}
