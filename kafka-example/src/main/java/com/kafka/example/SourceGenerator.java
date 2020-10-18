package com.kafka.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Properties;

/**
 * 自动发送数据
 * Created by wy on 2020/10/18.
 */
public class SourceGenerator {
    // 每秒1000条
    private static final long SPEED = 1000;
    // 最多发送条数
    private static final long THRESHOLD = 10000;
    private static final String TOPIC = "weibo_behavior";

    /**
     * 异步发送Kafka
     */
    public static void asyncSend(String key, String value) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, key, value);
        producer.send(record, new AsyncSendCallback());
        producer.close();
    }

    public static void main(String[] args) {
        // 发送速度
        Long speed = SPEED;
        // 最多发送条数
        Long threshold = THRESHOLD;
        if (args.length > 0) {
            speed = Long.valueOf(args[0]);
            threshold = Long.valueOf(args[1]);
        }

        // 每条耗时多少毫秒 = 1s(1000000ns) / 1000
        long delay = 1000_000 / speed;

        try (InputStream inputStream = SourceGenerator.class.getClassLoader().getResourceAsStream("weibo_data.txt")) {
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
            long start = System.nanoTime();
            int index = 1;
            while (reader.ready()) {
                String line = reader.readLine();
                String[] params = line.split("\\s+");
                String wid = params[1];
                // 异步发送
                asyncSend(wid, line);
                long end = System.nanoTime();
                long diff = end - start;
                while (diff < (delay*1000)) {
                    Thread.sleep(1);
                    end = System.nanoTime();
                    diff = end - start;
                }
                start = end;
                index ++;
                if(index >= threshold) {
                    break;
                }
            }
            reader.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
