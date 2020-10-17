package com.kafka.example;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;
import java.util.concurrent.Future;

/**
 * Kafka Producer Example
 * Created by wy on 2020/10/10.
 */
public class KafkaProducerExample {

    /**
     * 简单发送消息
     */
    public void simpleSend() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);
        String topic = "test";
        String key = "key-1";
        String value = "value-1";
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
        try {
            producer.send(record);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (producer != null) {
                producer.close();
            }
        }
    }

    /**
     * 同步发送消息
     */
    public void syncSend() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);
        String topic = "test";
        String key = "key-2";
        String value = "value-2";
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
        try {
            Future<RecordMetadata> metadataFuture = producer.send(record);
            RecordMetadata metadata = metadataFuture.get();
            System.out.println("返回结果: topic->" + metadata.topic() + ", partition->" + metadata.partition() +  ", offset->" + metadata.offset());
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (producer != null) {
                producer.close();
            }
        }
    }

    /**
     * 异步发送消息
     */
    public void asyncSend() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);
        String topic = "test";
        String key = "key-3";
        String value = "value-3";
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
        producer.send(record, new AsyncSendCallback());
        producer.close();
    }


    public static void main(String[] args) {
        KafkaProducerExample example = new KafkaProducerExample();
        //example.simpleSend();
        //example.syncSend();
        example.asyncSend();
    }
}
