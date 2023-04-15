package com.flink.example.stream.connector.kafka.base;

import com.common.example.bean.WordCount;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

/**
 * 功能：从 Kafka 中消费数据输出到 Kafka 示例
 * 作者：SmartSi
 * CSDN博客：https://blog.csdn.net/sunnyyoona
 * 公众号：大数据生态
 * 日期：2022/8/23 上午8:43
 */
public class KafkaToKafkaExample {

    private static final Gson gson = new GsonBuilder().create();
    private static final Logger LOG = LoggerFactory.getLogger(KafkaToKafkaExample.class);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 开启 Checkpoint 用于容错
        env.enableCheckpointing(10*1000);

        // Kafka Consumer 配置
        Properties consumerProps = new Properties();
        consumerProps.put("bootstrap.servers", "localhost:9092");
        consumerProps.put("group.id", "word-count");

        // 创建 Kafka Consumer
        String consumerTopic = "word";
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(consumerTopic, new SimpleStringSchema(), consumerProps);
        consumer.setStartFromLatest();
        DataStreamSource<String> sourceStream = env.addSource(consumer);

        // 单词计数
        DataStream<String> wordCountStream = sourceStream.map(new MapFunction<String, WordCount>() {
                    @Override
                    public WordCount map(String word) throws Exception {
                        return gson.fromJson(word, WordCount.class);
                    }
                })
                .keyBy(wc -> wc.getWord())
                .sum("frequency")
                .map(new MapFunction<WordCount, String>() {
                    @Override
                    public String map(WordCount wordCount) throws Exception {
                        return gson.toJson(wordCount);
                    }
                });


        // Kafka Producer 配置
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

        // 创建 Kafka Producer
        FlinkKafkaProducer<String> producer = new FlinkKafkaProducer<>(
                producerTopic, serializationSchema,
                producerProps, FlinkKafkaProducer.Semantic.EXACTLY_ONCE
        );
        wordCountStream.addSink(producer);

        // Print 输出 为了作对比
        wordCountStream.print();

        env.execute();
    }
}
