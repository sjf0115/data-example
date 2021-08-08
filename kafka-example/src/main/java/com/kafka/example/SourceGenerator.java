package com.kafka.example;

import com.common.example.bean.Behavior;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
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
    private static final Gson gson = new GsonBuilder().create();
    // 每秒1000条
    private static final long SPEED = 1;
    // 最多发送条数
    private static final long THRESHOLD = 1000;
    private static final String TOPIC = "behavior";

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

        try (InputStream inputStream = SourceGenerator.class.getClassLoader().getResourceAsStream("behavior.txt")) {
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
            long start = System.nanoTime();
            int index = 1;
            while (reader.ready()) {
                String line = reader.readLine();
                String[] params = line.split("\\t");
                String uid = params[0];
                String wid = params[1];
                String time = params[2];
                String content = params[3];
                Behavior behavior = new Behavior(uid, wid, time, content);
                String value = gson.toJson(behavior);
                // 异步发送
                System.out.println(value);
                asyncSend(uid, value);
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
