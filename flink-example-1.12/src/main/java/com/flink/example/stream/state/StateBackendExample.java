package com.flink.example.stream.state;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * 功能：
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/2/18 下午9:11
 */
public class StateBackendExample {
    private static final Logger LOG = LoggerFactory.getLogger(StateBackendExample.class);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置 Checkpoint 1s一次
        env.enableCheckpointing(1000L);

        // 在事件时间模式下使用处理时间语义
        env.getConfig().setAutoWatermarkInterval(0);

        // Checkpoint 存储路径
        String checkpointPath = "hdfs://localhost:9000/flink/checkpoints";

        // 设置 StateBackend
        // 1. MemoryStateBackend
        // env.setStateBackend(new MemoryStateBackend());

        // 2. MemoryStateBackend
        // env.setStateBackend(new FsStateBackend(checkpointPath));

        // 3. RocksDBStateBackend
        // env.setStateBackend(new RocksDBStateBackend(new FsStateBackend(checkpointPath), TernaryBoolean.TRUE));
        env.setStateBackend(new RocksDBStateBackend(new MemoryStateBackend()));

        // Source
        DataStream<String> source = env.socketTextStream("localhost", 9100, "\n");

        // Sum
        DataStream<Tuple2<String, Long>> result = source.keyBy(new KeySelector<String, String>() {
            @Override
            public String getKey(String uid) throws Exception {
                System.out.println("uid: " + uid);
                return uid;
            }
        }).map(new SumMapFunction());

        result.print();
        env.execute("StateBackendExample");
    }

    // SUM
    public static class SumMapFunction extends RichMapFunction<String, Tuple2<String, Long>> {
        private ValueState<Long> counterState;
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            ValueStateDescriptor<Long> stateDescriptor = new ValueStateDescriptor<>("counter", Long.class);
            counterState = getRuntimeContext().getState(stateDescriptor);
        }

        @Override
        public Tuple2<String, Long> map(String uid) throws Exception {
            Long count = counterState.value();
            if (Objects.equals(count, null)) {
                count = 0L;
            }
            Long newCount = count + 1;
            counterState.update(newCount);
            LOG.info("uid: {}, count: {}", uid, newCount);
            System.out.println(uid + ": " + newCount);
            return new Tuple2<>(uid, newCount);
        }
    }

}
