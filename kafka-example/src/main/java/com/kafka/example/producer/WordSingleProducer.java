package com.kafka.example.producer;

import com.common.example.bean.WordCount;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.kafka.clients.producer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;

/**
 * 自动发送数据
 * Created by wy on 2020/10/18.
 */
public class WordSingleProducer {
    private static final Logger LOG = LoggerFactory.getLogger(WordSingleProducer.class);
    private static final Gson gson = new GsonBuilder().create();

    /**
     * 单次发送
     * @param producer
     * @param key
     * @param value
     */
    public static void asyncSendSingle(Producer<String, String> producer, String topic, String key, String value) {
        // 异步发送
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if (e != null) {
                    e.printStackTrace();
                }
                if (recordMetadata != null) {
                    String topic = recordMetadata.topic();
                    int partition = recordMetadata.partition();
                    long offset = recordMetadata.offset();
                    LOG.info("topic: {}, partition: {}, offset: {}", topic, partition, offset);
                }
            }
        });
    }

    public static void main(String[] args) {
        String topic = "word";
//        List<String> words = Lists.newArrayList("a", "b", "a");
        List<String> words = Lists.newArrayList("a", "c", "ERROR");

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
                asyncSendSingle(producer, topic, word, value);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }
}
