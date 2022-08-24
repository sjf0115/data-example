package com.flink.example.stream.connector.kafka;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * 功能：Kafka End To End ExactlyOnce 示例
 * 作者：SmartSi
 * CSDN博客：https://blog.csdn.net/sunnyyoona
 * 公众号：大数据生态
 * 日期：2022/8/23 上午8:43
 */
public class KafkaEndToEndExactlyOnceExample {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaEndToEndExactlyOnceExample.class);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Checkpoint 设置
        env.enableCheckpointing(60*1000);
        // 重启与恢复策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                3, // 最多重启次数
                Time.of(10, TimeUnit.SECONDS) // 每次重启的时间间隔
        ));

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
                    private Random random = new Random();
                    @Override
                    public void flatMap(String value, Collector out) {
                        for (String word : value.split("\\s")) {
                            int num = random.nextInt(5);
                            if (num == 3) {
                                System.out.println("[word] " + word + " 异常");
                                throw new RuntimeException("模拟随机异常");
                            } else {
                                System.out.println("[word] " + word + " 正常");
                            }
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
        producerProps.put("transaction.timeout.ms", "5000");

        String producerTopic = "word-count-output";
        KafkaSerializationSchema<String> serializationSchema = new KafkaSerializationSchema<String>() {
            @Override
            public ProducerRecord<byte[], byte[]> serialize(String element, @Nullable Long timestamp) {
                return new ProducerRecord<>(producerTopic, element.getBytes(StandardCharsets.UTF_8)
                );
            }
        };

        FlinkKafkaProducer<String> producer = new FlinkKafkaProducer<>(
                producerTopic, serializationSchema,
                producerProps, FlinkKafkaProducer.Semantic.EXACTLY_ONCE
        );
        resultStream.addSink(producer);

        // Print 输出 为了作对比
        resultStream.print();

        env.execute();
    }
}
