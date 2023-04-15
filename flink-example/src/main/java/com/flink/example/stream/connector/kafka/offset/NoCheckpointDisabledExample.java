package com.flink.example.stream.connector.kafka.offset;

import com.flink.example.stream.connector.kafka.serializable.CustomKafkaDeserializationSchema;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * 功能：未开启 Checkpoint 下的 DISABLED 提交 Offset 模式
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2021/9/12 下午4:48
 */
public class NoCheckpointDisabledExample {

    private static final Logger LOG = LoggerFactory.getLogger(NoCheckpointDisabledExample.class);
    private static final Gson gson = new GsonBuilder().create();

    public static void main(String[] args) throws Exception{
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "no-checkpoint-disabled-example");
        // 禁用自动提交 Offset
        props.put("enable.auto.commit", "false");
        // 如果无分区 Offset，自动将 Offset 设置为最早 Offset
        props.put("auto.offset.reset", "earliest");
        //props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        String topic = "behavior";
        FlinkKafkaConsumer<ConsumerRecord<String, String>> consumer = new FlinkKafkaConsumer<>(
                topic,
                new CustomKafkaDeserializationSchema(),
                props
        );
        // 默认从消费者组最近一次提交的 Offset 开始消费。
        // 如果找不到分区 Offset，那么将会使用配置中的 auto.offset.reset 设置
        consumer.setStartFromGroupOffsets();

        SingleOutputStreamOperator<String> behavior = env.addSource(consumer)
                .setParallelism(1)
                .map(new MapFunction<ConsumerRecord<String, String>, String>() {
                    @Override
                    public String map(ConsumerRecord<String, String> record) throws Exception {
                        LOG.info("[Map] Topic: {}, Partition: {}, Offset: {}, Value: {}",
                                record.topic(), record.partition(), record.offset(), record.value()
                        );
                        return record.value();
                    }
                });

        behavior.print();
        env.execute("no-checkpoint-disabled-example");
    }
}
