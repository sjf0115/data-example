package com.flink.example.stream.state.checkpoint;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Objects;

/**
 * 从Checkpoint中恢复作业
 * Created by wy on 2020/12/26.
 */
public class RestoreCheckpointExample {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 配置Checkpoint
        env.enableCheckpointing(1000);
        env.setStateBackend(new FsStateBackend("hdfs://localhost:9000/flink/checkpoint"));
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // 配置失败重启策略：失败后最多重启3次 每次重启间隔10s
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 10000));

        DataStream<String> source = env.socketTextStream("localhost", 9100, "\n")
                .name("MySourceFunction");
        DataStream<Tuple2<String, Integer>> wordsCount = source.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector out) {
                // 失败信号
                if (Objects.equals(value, "ERROR")) {
                    throw new RuntimeException("custom error flag, restart application");
                }
                // 拆分单词
                for (String word : value.split("\\s")) {
                    out.collect(Tuple2.of(word, 1));
                }
            }
        }).name("MyFlatMapFunction");

        DataStream<Tuple2<String, Integer>> windowCount = wordsCount
                .keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Integer> tuple) throws Exception {
                        return tuple.f0;
                    }
                })
                .sum(1).name("MySumFunction");

        windowCount.print().setParallelism(1).name("MyPrintFunction");
        env.execute("RestoreCheckpointExample");
    }
}
