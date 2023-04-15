package com.flink.example.stream.connector.kafka.serializable;

import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;

/**
 * 功能：自定义 Kafka 序列化器
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2023/4/15 下午9:42
 */
public class CustomKafkaSerializationSchema implements KafkaSerializationSchema<ProducerRecord<String, String>> {
    @Override
    public ProducerRecord<byte[], byte[]> serialize(ProducerRecord<String, String> record, @Nullable Long timestamp) {
        return new ProducerRecord<>(
                record.topic(),
                record.key().getBytes(StandardCharsets.UTF_8),
                record.value().getBytes(StandardCharsets.UTF_8)
        );
    }
}
