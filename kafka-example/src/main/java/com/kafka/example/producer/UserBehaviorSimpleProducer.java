package com.kafka.example.producer;

import com.common.example.bean.UserBehavior;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.kafka.example.consumer.AsyncSendCallback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * 功能：淘宝用户行为发送 Kafka 简单发送
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/5/3 下午10:16
 */
public class UserBehaviorSimpleProducer {
    private static final Gson gson = new GsonBuilder().create();
    private static final String TOPIC = "user_behavior";

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);

        String line = "1001,3827899,2920476,pv,1511713473000,2017-11-27 00:24:33";
//        String line = "1001,3745169,2891509,pv,1511714671000,2017-11-27 00:44:31";
//        String line = "1002,266784,2520771,pv,1511715833000,2017-11-27 01:03:53";
//        String line = "1002,2286574,2465336,pv,1511797167000,2017-11-27 01:13:27";
//        String line = "1001,1531036,2920476,pv,1511718252000,2017-11-27 01:44:12";
//        String line = "1001,2266567,4145813,pv,1511741471000,2017-11-27 08:11:11";
//        String line = "1001,2951368,1080785,pv,1511750828000,2017-11-27 10:47:08";

        // CSV 格式
//        String[] params = line.split(",");
//        String key = params[0];
//        String value = line;

        // Json 格式
        String[] params = line.split(",");
        Long uid = Long.parseLong(params[0]);
        Long pid = Long.parseLong(params[1]);
        Long cid = Long.parseLong(params[2]);
        String type = params[3];
        Long ts = Long.parseLong(params[4]);
        String time = params[5];
        UserBehavior userBehavior = new UserBehavior(uid, pid, cid, type, ts, time);
        String key = String.valueOf(uid);
        String value = gson.toJson(userBehavior);

        // 发送
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, key, value);
        producer.send(record, new AsyncSendCallback());
        producer.close();
    }
}
//1001,3827899,2920476,pv,1511713473000,2017-11-27 00:24:33
//1001,3745169,2891509,pv,1511714671000,2017-11-27 00:44:31
//1002,266784,2520771,pv,1511715833000,2017-11-27 01:03:53
//1002,2286574,2465336,pv,1511797167000,2017-11-27 01:13:27
//1001,1531036,2920476,pv,1511718252000,2017-11-27 01:44:12
//1001,2266567,4145813,pv,1511741471000,2017-11-27 08:11:11
//1001,2951368,1080785,pv,1511750828000,2017-11-27 10:47:08

