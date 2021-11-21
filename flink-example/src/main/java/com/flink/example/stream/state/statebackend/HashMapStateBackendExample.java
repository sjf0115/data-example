package com.flink.example.stream.state.statebackend;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
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
        // 设置状态后端
        env.setStateBackend(new HashMapStateBackend());
        // 设置 Checkpoint
        env.enableCheckpointing(1000L);
        FileSystemCheckpointStorage checkpointStorage = new FileSystemCheckpointStorage("hdfs://localhost:9000/flink/checkpoint");
        env.getCheckpointConfig().setCheckpointStorage(checkpointStorage);

        DataStream<String> source = env.socketTextStream("localhost", 9100, "\n");

        DataStream<Tuple2<String, Long>> result = source.map(new MapFunction<String, Tuple2<String, String>>() {
            // 清洗
            @Override
            public Tuple2<String, String> map(String value) throws Exception {
                String[] params = value.split(",");
                String time = params[0];
                String key = params[1];
                LOG.info("[Input] Key: {}, Time: {}", key, time);
                return new Tuple2<>(time, key);
            }
        }).keyBy(new KeySelector<Tuple2<String, String>, String>() {
            // 分组
            @Override
            public String getKey(Tuple2<String, String> tuple) throws Exception {
                return tuple.f1;
            }
        }).map(new RichMapFunction<Tuple2<String, String>, Tuple2<String, Long>>() {
            // 计数器
            private ValueState<Long> counterState;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                ValueStateDescriptor<Long> stateDescriptor = new ValueStateDescriptor<>("counter", Long.class);
                counterState = getRuntimeContext().getState(stateDescriptor);
            }

            @Override
            public Tuple2<String, Long> map(Tuple2<String, String> tuple) throws Exception {
                String key = tuple.f1;
                Long count = counterState.value();
                if (Objects.equals(count, null)) {
                    count = 0L;
                }
                Long newCount = count + 1;
                counterState.update(newCount);
                LOG.info("[Output] Key: {}, Count: {}", key, newCount);
                return new Tuple2<>(key, newCount);
            }
        });

        result.print();

        env.execute("HashMapStateBackendExample");
    }
}

// 2021-10-30 11:21:00,a
// 2021-10-30 11:21:01,a
// 2021-10-30 11:21:02,c
// 2021-10-30 11:21:03,d
// 2021-10-30 11:21:04,a
// 2021-10-30 11:21:05,d
// 2021-10-30 11:21:06,c