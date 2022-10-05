package com.kafka.example.producer;

import com.common.example.bean.UserBehavior;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.kafka.example.consumer.AsyncSendCallback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.List;
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
    private static final Long SLEEP_TIME = 5*1000L;
    private static final Long SESSION_SLEEP_TIME = 6*1000L;

    public static void send(boolean isSessionWindow) {
        // 配置
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // 输入数据
        List<String> elements = Lists.newArrayList(
                "1001,3827899,2920476,pv,1664636572000,2022-10-01 23:02:52",
                "1001,3745169,2891509,pv,1664636570000,2022-10-01 23:02:50",
                "1001,266784,2520771,pv,1664636573000,2022-10-01 23:02:53",
                "1001,2286574,2465336,pv,1664636574000,2022-10-01 23:02:54",
                "1001,1531036,2920476,pv,1664636577000,2022-10-01 23:02:57",
                "1001,2266567,4145813,pv,1664636584000,2022-10-01 23:03:04",
                "1001,2951368,1080785,pv,1664636576000,2022-10-01 23:02:56",
                "1001,3658601,2342116,pv,1664636586000,2022-10-01 23:03:06",
                "1001,5153036,2342116,pv,1664636578000,2022-10-01 23:02:58",
                "1001,598929,2429887,pv,1664636591000,2022-10-01 23:03:11",
                "1001,3245421,2881542,pv,1664636595000,2022-10-01 23:03:15",
                "1001,1046201,3002561,pv,1664636579000,2022-10-01 23:02:59",
                "1001,2971043,4869428,pv,1664636646000,2022-10-01 23:04:06"
        );

        // 发送
        Producer<String, String> producer = new KafkaProducer<>(props);
        int index = 1;
        for (String element : elements) {
            // CSV 格式
            //        String[] params = element.split(",");
            //        String key = params[0];
            //        String value = line;
            // Json 格式
            String[] params = element.split(",");
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
            try {
                // SessionWindow 场景下 第二个和第6个数据记录后增加1s休眠时间
                if(isSessionWindow && (index == 2 || index == 6)) {
                    // 每6s输出一次
                    Thread.sleep(SESSION_SLEEP_TIME);
                } else {
                    // 每5s输出一次
                    Thread.sleep(SLEEP_TIME);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            index ++;
        }
        producer.close();
    }

    public static void main(String[] args) {
        // 用于会话窗口
        send(true);
        // 用于滚动,滑动窗口
        //send(false);
    }
}

