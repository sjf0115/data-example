package com.kafka.example;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * 异步发送消息回调
 * Created by wy on 2020/10/10.
 */
public class AsyncSendCallback implements Callback {
    @Override
    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
        if (e != null) {
            e.printStackTrace();
        }
        if (recordMetadata != null) {
            System.out.println("返回结果: topic->" + recordMetadata.topic() + ", partition->" + recordMetadata.partition() +  ", offset->" + recordMetadata.offset());
        }
    }
}
