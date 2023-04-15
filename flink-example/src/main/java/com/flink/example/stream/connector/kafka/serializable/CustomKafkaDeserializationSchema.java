package com.flink.example.stream.connector.kafka.serializable;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * 功能：自定义 Kafka 反序列化器
 * 作者：SmartSi
 * 博客：https://blog.csdn.net/sunnyyoona
 * 公众号：大数据生态
 * 日期：2021/9/12 下午6:04
 */
public class CustomKafkaDeserializationSchema implements KafkaDeserializationSchema<ConsumerRecord<String, String>> {
    // 是否表示流的最后一条元素，我们要设置为 false ,因为我们需要 msg 源源不断的被消费
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

    // 告诉 Flink 我输入的数据类型, 方便 Flink 的类型推断
    @Override
    public TypeInformation<ConsumerRecord<String, String>> getProducedType() {
        return TypeInformation.of(new TypeHint<ConsumerRecord<String, String>>() {
        });
    }
}
