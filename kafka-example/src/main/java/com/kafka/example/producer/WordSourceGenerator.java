package com.kafka.example.producer;

import com.common.example.bean.WordCount;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.kafka.example.consumer.AsyncSendCallback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.List;
import java.util.Properties;
import java.util.Random;

/**
 * 自动发送数据
 * Created by wy on 2020/10/18.
 */
public class WordSourceGenerator {
    private static final Gson gson = new GsonBuilder().create();
    // 每秒1条
    private static final long SPEED = 1;
    // 最多发送条数
    private static final long THRESHOLD = 20;
    private static final String TOPIC = "word";
    private static List<String> words = Lists.newArrayList("hello", "word", "flink", "spark", "storm");

    /**
     * 异步发送Kafka
     */
    public static void asyncSend() throws InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);

        // 每条耗时多少毫秒 = 1s(1000000ns) / 1000
        long delay = 1000_000 / SPEED;
        long start = System.nanoTime();
        int index = 1;
        Random random = new Random();
        while (true) {
            int wordIndex = random.nextInt(4);
            String word = words.get(wordIndex);
            WordCount wordCount = new WordCount(word, 1L);
            String value = gson.toJson(wordCount);

            // 异步发送
            ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, word, value);
            producer.send(record, new AsyncSendCallback());
            System.out.println(value);

            long end = System.nanoTime();
            long diff = end - start;
            while (diff < (delay*1000)) {
                Thread.sleep(1);
                end = System.nanoTime();
                diff = end - start;
            }
            start = end;
            index ++;
            if(index >= THRESHOLD) {
                break;
            }
        }

        producer.close();
    }

    public static void main(String[] args) throws InterruptedException {
        asyncSend();
    }
}
