package com.flink.example.stream.connector.kafka.serializable;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * 功能：自定义Kafka序列化器
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2021/9/12 下午6:04
 */
public class CustomKafkaDeserializationSchema implements KafkaDeserializationSchema<ConsumerRecord<String, String>> {

    @Override
    public boolean isEndOfStream(ConsumerRecord<String, String> nextElement) {
        return false;
    }

    @Override
    public ConsumerRecord<String, String> deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
        ConsumerRecord<String, String> consumerRecord = new ConsumerRecord<>(
                record.topic(),
                record.partition(),
                record.offset(),
                new String(record.key(), "UTF-8"),
                new String(record.value(), "UTF-8")
        );
        return consumerRecord;
    }

    @Override
    public TypeInformation<ConsumerRecord<String, String>> getProducedType() {
        return TypeInformation.of(new TypeHint<ConsumerRecord<String, String>>() {
        });
    }
}
