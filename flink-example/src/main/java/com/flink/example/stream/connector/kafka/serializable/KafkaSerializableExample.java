package com.flink.example.stream.connector.kafka.serializable;

import com.common.example.bean.WordCount;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

/**
 * 功能：Kafka KafkaSerializationSchema 示例
 *      使用 SimpleStringSchema 反序列化
 *      使用 KafkaSerializationSchema 序列化
 * 作者：SmartSi
 * CSDN博客：https://blog.csdn.net/sunnyyoona
 * 公众号：大数据生态
 * 日期：2022/8/23 上午8:43
 */
public class KafkaSerializableExample {

    private static final Gson gson = new GsonBuilder().create();
    private static final Logger LOG = LoggerFactory.getLogger(KafkaSerializableExample.class);

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
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(
                consumerTopic,
                // 使用 SimpleStringSchema 反序列化
                new SimpleStringSchema(),
                consumerProps
        );
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

        // 创建 Kafka Producer
        String producerTopic = "word-count-output";
        String key = "word";
        FlinkKafkaProducer<String> producer = new FlinkKafkaProducer<>(
                producerTopic,
                // 使用 KafkaSerializationSchema 序列化
                new KafkaSerializationSchema<String>() {
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(String element, @Nullable Long timestamp) {
                        return new ProducerRecord<>(
                                producerTopic,
                                key.getBytes(StandardCharsets.UTF_8),
                                element.getBytes(StandardCharsets.UTF_8)
                        );
                    }
                },
                producerProps,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE
        );
        wordCountStream.addSink(producer);

        // Print 输出 为了作对比
        wordCountStream.print();

        env.execute();
    }
}
