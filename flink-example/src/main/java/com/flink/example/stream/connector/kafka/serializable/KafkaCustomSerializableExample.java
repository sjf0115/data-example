package com.flink.example.stream.connector.kafka.serializable;

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
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

/**
 * 功能：Kafka 自定义序列化器和反序列化器示例
 * 作者：SmartSi
 * CSDN博客：https://blog.csdn.net/sunnyyoona
 * 公众号：大数据生态
 * 日期：2022/8/23 上午8:43
 */
public class KafkaCustomSerializableExample {

    private static final Gson gson = new GsonBuilder().create();
    private static final Logger LOG = LoggerFactory.getLogger(KafkaCustomSerializableExample.class);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 开启 Checkpoint 用于容错
        env.enableCheckpointing(10*1000);

        // Topic
        String consumerTopic = "word";
        String producerTopic = "word-count-output";

        // 创建 Kafka Consumer
        Properties consumerProps = new Properties();
        consumerProps.put("bootstrap.servers", "localhost:9092");
        consumerProps.put("group.id", "word-count");
        FlinkKafkaConsumer<ConsumerRecord<String, String>> consumer = new FlinkKafkaConsumer<>(
                consumerTopic,
                // 自定义反序列化器
                new CustomKafkaDeserializationSchema(),
                consumerProps
        );
        consumer.setStartFromLatest();
        DataStreamSource<ConsumerRecord<String, String>> sourceStream = env.addSource(consumer);

        // 单词计数
        DataStream<ProducerRecord<String, String>> wordCountStream = sourceStream.map(new MapFunction<ConsumerRecord<String, String>, WordCount>() {
                    @Override
                    public WordCount map(ConsumerRecord<String, String> record) throws Exception {
                        LOG.info("[INFO] record topic: {}, partition: {}, offset: {}, key: {}, value: {}, timestamp: {}",
                                record.topic(),
                                record.partition(),
                                record.offset(),
                                record.key(),
                                record.value(),
                                record.timestamp()
                        );
                        return gson.fromJson(record.value(), WordCount.class);
                    }
                })
                .keyBy(wc -> wc.getWord())
                .sum("frequency")
                .map(new MapFunction<WordCount, ProducerRecord<String, String>>() {
                    @Override
                    public ProducerRecord<String, String> map(WordCount wordCount) throws Exception {
                        ProducerRecord<String, String> record = new ProducerRecord<>(
                                producerTopic,
                                wordCount.getWord(),
                                gson.toJson(wordCount)
                        );
                        return record;
                    }
                });

        // 创建 Kafka Producer
        Properties producerProps = new Properties();
        producerProps.put("bootstrap.servers", "localhost:9092");
        producerProps.put("transaction.timeout.ms", "5000");
        FlinkKafkaProducer<ProducerRecord<String, String>> producer = new FlinkKafkaProducer<>(
                producerTopic,
                // 自定义序列化器
                new CustomKafkaSerializationSchema(),
                producerProps,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE
        );
        wordCountStream.addSink(producer);

        env.execute();
    }
}
