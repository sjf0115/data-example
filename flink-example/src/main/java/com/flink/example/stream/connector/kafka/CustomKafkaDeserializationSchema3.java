package com.flink.example.stream.connector.kafka;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * 功能：自定义Kafka序列化器
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2021/9/12 下午6:04
 */
public class CustomKafkaDeserializationSchema3 implements KafkaDeserializationSchema<Tuple4<String, Integer, Long, String>> {

    @Override
    public boolean isEndOfStream(Tuple4<String, Integer, Long, String> nextElement) {
        return false;
    }

    @Override
    public Tuple4<String, Integer, Long, String> deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
        return new Tuple4<>(
                record.topic(),
                record.partition(),
                record.offset(),
                new String(record.value(), "UTF-8")
        );
    }

    @Override
    public TypeInformation<Tuple4<String, Integer, Long, String>> getProducedType() {
        return new TupleTypeInfo<>(
                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.INT_TYPE_INFO,
                BasicTypeInfo.LONG_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO
        );
    }
}
