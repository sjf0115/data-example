package com.kafka.example.utils;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 功能：发送工具类
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2022/10/10 下午11:17
 */
public class SendUtil {
    private static final Logger LOG = LoggerFactory.getLogger(SendUtil.class);
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
}
