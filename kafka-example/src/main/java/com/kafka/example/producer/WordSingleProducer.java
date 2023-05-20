package com.kafka.example.producer;

import com.common.example.bean.WordCount;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.kafka.example.utils.SendUtil;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;

import java.util.List;
import java.util.Properties;

/**
 * 自动发送数据
 * Created by wy on 2020/10/18.
 */
public class WordSingleProducer {
    private static final Gson gson = new GsonBuilder().create();

    public static void main(String[] args) {
        String topic = "word";
        List<String> words = Lists.newArrayList("b", "c");
//        List<String> words = Lists.newArrayList("a", "c", "ERROR");

        // 配置发送者
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        Producer<String, String> producer = null;
        try {
            producer = new KafkaProducer<>(props);
            for (String word : words) {
                WordCount wordCount = new WordCount(word, 1L);
                String value = gson.toJson(wordCount);
                SendUtil.asyncSendSingle(producer, topic, word, value);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }
}
