package com.flink.example.stream.function;

import com.common.example.bean.WordCount;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 功能：KeyBy 演示
 * 作者：SmartSi
 * CSDN博客：https://blog.csdn.net/sunnyyoona
 * 公众号：大数据生态
 * 日期：2022/8/18 下午10:47
 */
public class KeyByExample {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> stream = env.socketTextStream("localhost", 9100, "\n");
        // 单词流
        DataStream<Tuple2<String, Integer>> wordsCount = stream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector out) {
                for (String word : value.split("\\s")) {
                    out.collect(Tuple2.of(word, 1));
                }
            }
        });

        // (1) 使用 Tuple 下标位置进行 KeyBy
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream1 = wordsCount.keyBy(0);
        keyedStream1.sum(1).print("KeyedStream1");

        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream2 = wordsCount.keyBy(0, 1);
        keyedStream2.sum(1).print("KeyedStream2");

        // (2) 通过 Tuple 字段名称进行 KeyBy
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream3 = wordsCount.keyBy("f0");
        keyedStream3.sum(1).print("KeyedStream3");

        // (3) 通过 POJO 字段名称进行 KeyBy
        DataStream<WordCount> wordsCount2 = stream.flatMap(new FlatMapFunction<String, WordCount>() {
            @Override
            public void flatMap(String value, Collector out) {
                for (String word : value.split("\\s")) {
                    out.collect(new WordCount(word, 1L));
                }
            }
        });
        KeyedStream<WordCount, Tuple> keyedStream4 = wordsCount2.keyBy("word");
        keyedStream4.sum("frequency").print("KeyedStream4");

        // (4) 使用 KeySelector 进行 KeyBy
        KeyedStream<Tuple2<String, Integer>, String> keyedStream5 = wordsCount.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> tuple2) throws Exception {
                return tuple2.f0;
            }
        });
        keyedStream5.sum(1).print("KeyedStream5");

        env.execute("KeyByExample");
    }
}
