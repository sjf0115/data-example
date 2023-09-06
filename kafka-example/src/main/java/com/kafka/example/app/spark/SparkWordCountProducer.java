package com.kafka.example.app.spark;

import com.google.common.collect.Lists;
import com.kafka.example.utils.SendUtil;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;

import java.util.List;
import java.util.Properties;

/**
 * 功能：Spark Streaming WordCount 数据
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2023/9/6 07:26
 */
public class SparkWordCountProducer {

    public static void main(String[] args) {
        String topic = "spark-words";
        List<String> words = Lists.newArrayList("I am a Student", "I like eating apple", "Student like eating apple");

        // 配置发送者
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        Producer<String, String> producer = null;
        try {
            producer = new KafkaProducer<>(props);
            for (String word : words) {
                SendUtil.asyncSendSingle(producer, topic, "", word);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }
}
