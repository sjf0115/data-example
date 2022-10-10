package com.kafka.example.producer;

import com.google.common.collect.Lists;
import com.kafka.example.utils.SendUtil;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;

import java.util.List;
import java.util.Properties;

/**
 * 自动发送数据
 * Created by wy on 2020/10/18.
 */
public class TextSingleProducer {
    public static void main(String[] args) {
        String topic = "word";
        List<String> lines = Lists.newArrayList("hello world", "hello spark", "hello flink");

        // 配置发送者
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        Producer<String, String> producer = null;
        try {
            producer = new KafkaProducer<>(props);
            for (String line : lines) {
                SendUtil.asyncSendSingle(producer, topic, "word", line);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }
}
