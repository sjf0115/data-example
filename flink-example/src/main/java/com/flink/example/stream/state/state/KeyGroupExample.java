package com.flink.example.stream.state.state;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Created by wy on 2021/3/7.
 */
public class KeyGroupExample {

    private static final Logger LOG = LoggerFactory.getLogger(KeyGroupExample.class);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置事件时间特性
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<String> source = env.socketTextStream("localhost", 9100, "\n");

        source.flatMap(new FlatMapFunction<String, Tuple2<String, String>>() {
            // 拆分
            @Override
            public void flatMap(String str, Collector out) {
                String[] params = str.split(",");
                out.collect(Tuple2.of(params[0], params[1]));
            }
        }).keyBy(new KeySelector<Tuple2<String,String>, String>() {
            // 分组
            @Override
            public String getKey(Tuple2<String, String> tuple) throws Exception {
                return tuple.f0;
            }
        }).map(new MapFunction<Tuple2<String,String>, String>() {
            int parallelism = 3;
            int maxParallelism = KeyGroupRangeAssignment.computeDefaultMaxParallelism(parallelism);
            // Map
            @Override
            public String map(Tuple2<String, String> tuple2) throws Exception {
                int keyGroupId = KeyGroupRangeAssignment.computeKeyGroupForKeyHash(tuple2.f0.hashCode(), maxParallelism);
                int subTaskId = KeyGroupRangeAssignment.computeOperatorIndexForKeyGroup(maxParallelism, parallelism, keyGroupId);

                LOG.info("Parallelism: {}, MaxParallelism: {}, Key: {}, KeyGroupId: {}, SubTaskId: {}",
                        parallelism, maxParallelism, tuple2.f0, keyGroupId, subTaskId
                );

                return tuple2.f0 + " -> " + tuple2.f1;
            }
        }).setParallelism(3);

        env.execute("KeyGroupExample");

        // 算子并发：3
        int parallelism = 3;

        // 最大并：128 对应有 128 个 KeyGroup
        int maxParallelism = KeyGroupRangeAssignment.computeDefaultMaxParallelism(parallelism);
        System.out.println(maxParallelism);

        for (int i = 0; i < parallelism;i++) {
            KeyGroupRange keyGroupRange = KeyGroupRangeAssignment.computeKeyGroupRangeForOperatorIndex(maxParallelism, parallelism, i);
            System.out.println("SubTask" + i + " -> KeyGroupRange [" + keyGroupRange.getStartKeyGroup() + ", " + keyGroupRange.getEndKeyGroup() + "]");
        }

        // 通过KeyGroupId映射SubTaskId(OperatorIndex)
        int subTaskId = KeyGroupRangeAssignment.computeOperatorIndexForKeyGroup(maxParallelism, parallelism, 2);
        System.out.println("KeyGroupId: 2 -> SubTaskId: " + subTaskId);

        // 通过SubTaskId(OperatorIndex)映射KeyGroupId
        KeyGroupRange keyGroupRange = KeyGroupRangeAssignment.computeKeyGroupRangeForOperatorIndex(maxParallelism, parallelism, 1);
        System.out.println("SubTaskId: 1 -> keyGroupRange: [" + keyGroupRange.getStartKeyGroup() + ", " + keyGroupRange.getEndKeyGroup() + "]");

        // Key: A 在 232 KeyGroup 上

        List<String> list = Lists.newArrayList("A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L");
        for(String key : list) {
            int keyGroupId = KeyGroupRangeAssignment.computeKeyGroupForKeyHash(key.hashCode(), maxParallelism);
            subTaskId = KeyGroupRangeAssignment.computeOperatorIndexForKeyGroup(maxParallelism, parallelism, keyGroupId);
            System.out.println("Key: " + key + ", KeyGroupId: " + keyGroupId + ", SubTaskId: " + subTaskId);
        }
    }
}
