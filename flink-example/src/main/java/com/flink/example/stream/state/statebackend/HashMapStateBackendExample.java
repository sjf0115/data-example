package com.flink.example.stream.state.statebackend;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.storage.JobManagerCheckpointStorage;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * 功能：HashMapStateBackend
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2021/10/30 上午8:47
 */
public class HashMapStateBackendExample {
    private static final Logger LOG = LoggerFactory.getLogger(HashMapStateBackendExample.class);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 在事件时间模式下使用处理时间语义
        env.getConfig().setAutoWatermarkInterval(0);

        // 设置状态后端
        env.setStateBackend(new HashMapStateBackend());

        // 设置Checkpoint存储
        env.enableCheckpointing(1000L);
        env.getCheckpointConfig().setCheckpointStorage(new JobManagerCheckpointStorage());

        DataStream<String> source = env.socketTextStream("localhost", 9100, "\n");

        DataStream<Tuple2<String, Long>> result = source.keyBy(new KeySelector<String, String>() {
            @Override
            public String getKey(String uid) throws Exception {
                return uid;
            }
        }).map(new RichMapFunction<String, Tuple2<String, Long>>() {
            // 计数器
            private ValueState<Long> counterState;
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                ValueStateDescriptor<Long> stateDescriptor = new ValueStateDescriptor<>("counter", Long.class);
                counterState = getRuntimeContext().getState(stateDescriptor);
            }

            @Override
            public Tuple2<String, Long> map(String key) throws Exception {
                Long count = counterState.value();
                if (Objects.equals(count, null)) {
                    count = 0L;
                }
                Long newCount = count + 1;
                counterState.update(newCount);
                LOG.info("Key: {}, Count: {}", key, newCount);
                return new Tuple2<>(key, newCount);
            }
        });

        result.print();

        env.execute("HashMapStateBackendExample");
    }
}
// 输入
// a
// a
// c
// d
// a
// d
// c

// 输出
//Key: a, Count: 1
//Key: a, Count: 2
//Key: c, Count: 1
//Key: d, Count: 1
//Key: a, Count: 3
//Key: d, Count: 2
//Key: c, Count: 2