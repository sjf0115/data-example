package com.flink.example.stream.connector.kafka;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * 功能：Kafka End To End ExactlyOnce 示例
 * 作者：SmartSi
 * CSDN博客：https://blog.csdn.net/sunnyyoona
 * 公众号：大数据生态
 * 日期：2022/8/23 上午8:43
 */
public class KafkaEndToEndExactlyOnceExample {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Kafka Source
        Properties consumerProps = new Properties();
        consumerProps.put("bootstrap.servers", "localhost:9092");
        consumerProps.put("group.id", "end-to-end-exactly-once-group");
        consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put("auto.offset.reset", "latest");

        String consumerTopic = "word-count-input";
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(
                consumerTopic, new SimpleStringSchema(), consumerProps
        );
        DataStreamSource<String> sourceStream = env.addSource(consumer);

        // 单词计数
        SingleOutputStreamOperator<String> resultStream = sourceStream
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String value, Collector out) {
                        for (String word : value.split("\\s")) {
                            out.collect(Tuple2.of(word, 1));
                        }
                    }
                })
                .keyBy(tuple2 -> tuple2.f0)
                .sum(1).map(new MapFunction<Tuple2<String, Integer>, String>() {
                    @Override
                    public String map(Tuple2<String, Integer> tuple2) throws Exception {
                        return tuple2.f0 + "," + tuple2.f1;
                    }
                });


        // Kafka Producer
        Properties producerProps = new Properties();
        producerProps.put("bootstrap.servers", "localhost:9092");
        //producerProps.put("group.id", "end-to-end-exactly-once-group");

        String producerTopic = "word-count-output";
        FlinkKafkaProducer<String> producer = new FlinkKafkaProducer<>(
                producerTopic, new KeyedSerializationSchemaWrapper<>(new SimpleStringSchema()),
                producerProps, FlinkKafkaProducer.Semantic.EXACTLY_ONCE
        );
        resultStream.addSink(producer);

        env.execute();
    }
}
