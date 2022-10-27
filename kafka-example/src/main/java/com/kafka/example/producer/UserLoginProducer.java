package com.kafka.example.producer;

import com.common.example.bean.LoginUser;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.kafka.example.utils.SendUtil;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;

/**
 * 模拟用户登录数据
 * Created by wy on 2020/10/18.
 */
public class UserLoginProducer {
    private static final Logger LOG = LoggerFactory.getLogger(UserLoginProducer.class);
    private static final Gson gson = new GsonBuilder().create();

    private static List<String> userLogin = Lists.newArrayList(
            "1,10001,android,1665417052000",  // 23:50:52
            "1,10002,iOS,1665417005000",      // 23:50:05
            "1,10003,android,1665417076000",  // 23:51:16
            "1,10002,android,1665417144000",  // 23:52:24
            "1,10001,android,1665417217000",  // 23:53:37
            "1,10004,iOS,1665417284000",      // 23:54:44
            "1,10003,android,1665417356000",  // 23:55:56
            "1,10005,android,1665417366000",  // 23:56:06
            "1,10006,android,1665417555000",  // 23:59:15
            "1,10007,iOS,1665417659000",      // 00:00:59
            "1,10001,android,1665417685000",  // 00:01:25
            "1,10008,android,1665417756000"   // 00:02:36
    );

    public static void main(String[] args) {
        String topic = "user_login";
        // 配置发送者
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        Producer<String, String> producer = null;
        try {
            producer = new KafkaProducer<>(props);
            for (String user : userLogin) {
                String[] params = user.split(",");
                Integer appId = Integer.parseInt(params[0]);
                Long uid = Long.parseLong(params[1]);
                String os = params[2];
                Long timestamp = Long.parseLong(params[3]);
                LOG.info("AppId: {}, uid: {}, os: {}, timestamp: {}", appId, uid, os, timestamp);
                String value = gson.toJson(new LoginUser(appId, uid, os, timestamp));
                SendUtil.asyncSendSingle(producer, topic, String.valueOf(appId), value);
                Thread.sleep(10*1000);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }
}
