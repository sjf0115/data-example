package com.kafka.example.producer;

import com.common.example.bean.ShopSales;
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
 * 功能：下单金额 Kafka 简单发送
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/5/3 下午10:16
 */
public class ShopSalesSimpleProducer {
    private static final Gson gson = new GsonBuilder().create();
    private static final String TOPIC = "shop_sales";
    private static final Long SLEEP_TIME = 5*1000L;

    public static void send() {
        // 配置
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // 输入数据
        List<String> elements = Lists.newArrayList(
                "1001,图书,40,1665360300000", // 2022-10-10 08:05:00
                "2001,生鲜,40,1665360360000", // 2022-10-10 08:06:00
                "1001,图书,20,1665360420000", // 2022-10-10 08:07:00
                "2002,生鲜,20,1665360480000", // 2022-10-10 08:08:00
                "2003,生鲜,50,1665360540000", // 2022-10-10 08:09:00
                "1002,图书,80,1665360600000", // 2022-10-10 08:10:00
                "2004,生鲜,20,1665360660000", // 2022-10-10 08:11:00
                "2005,生鲜,20,1665360720000", // 2022-10-10 08:12:00
                "1003,图书,10,1665360780000", // 2022-10-10 08:13:00
                "2006,生鲜,20,1665360840000", // 2022-10-10 08:14:00
                "1003,图书,30,1665360900000", // 2022-10-10 08:15:00
                "1006,图书,60,1665361020000", // 2022-10-10 08:17:00
                "1007,图书,90,1665361080000", // 2022-10-10 08:18:00
                "2007,生鲜,40,1665361260000" // 2022-10-10 08:21:00
        );

        // 发送
        Producer<String, String> producer = new KafkaProducer<>(props);
        for (String element : elements) {
            // Json 格式
            String[] params = element.split(",");
            Long productId = Long.parseLong(params[0]);
            String category = params[1];
            Long sales = Long.parseLong(params[2]);
            Long timestamp = Long.parseLong(params[3]);

            ShopSales shopSales = new ShopSales(productId, category, sales, timestamp);
            String key = String.valueOf(productId);
            String value = gson.toJson(shopSales);

            // 发送
            ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, key, value);
            producer.send(record, new AsyncSendCallback());
            try {
                // 每5s输出一次
                Thread.sleep(SLEEP_TIME);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        producer.close();
    }

    public static void main(String[] args) {
        send();
    }
}

