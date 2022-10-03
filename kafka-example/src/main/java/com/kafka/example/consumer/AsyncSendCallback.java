package com.kafka.example.consumer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 异步发送消息回调
 * Created by wy on 2020/10/10.
 */
public class AsyncSendCallback implements Callback {
    private static final Logger LOG = LoggerFactory.getLogger(AsyncSendCallback.class);

    @Override
    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
        if (e != null) {
            e.printStackTrace();
        }
        if (recordMetadata != null) {
            String topic = recordMetadata.topic();
            int partition = recordMetadata.partition();
            long offset = recordMetadata.offset();
            LOG.info("返回结果 topic: {}, partition: {}, offset: {}", topic, partition, offset);
        }
    }
}
