package com.kafka.example.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * 功能：提交指定分区 Offset
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2021/9/11 下午10:31
 */
public class CommitOffsetMapExample {
    private static final Logger LOG = LoggerFactory.getLogger(AsyncCommitOffsetExample.class);


    public static void main(String[] args) {
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        int count = 0;

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("group.id", "commit-offset-map-example");
        props.setProperty("enable.auto.commit", "false");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("behavior"));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("topic = %s, partition = %s, offset = %d, key = %s, value = %s\n",
                        record.topic(), record.partition(), record.offset(), record.key(), record.value()
                );
                offsets.put(
                        new TopicPartition(record.topic(), record.partition()),
                        new OffsetAndMetadata(record.offset() + 1, "no metadata")
                );
                // 每处理100条消息提交一次Offset
                if (count % 1000 == 0) {
                    consumer.commitAsync(offsets, new OffsetCommitCallback(){
                        @Override
                        public void onComplete(Map<TopicPartition, OffsetAndMetadata> map, Exception e) {
                            if (e != null) {
                                LOG.error("Commit failed for offsets {}", offsets, e);
                            }
                        }
                    });
                }
                count ++;
            }
        }
    }
}
