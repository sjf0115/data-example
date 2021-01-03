package com.flink.example.stream.state.savepoint;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.Objects;

/**
 * 拓扑变化
 * Created by wy on 2021/1/2.
 */
public class TopologyChangesExample {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStateBackend((StateBackend) new FsStateBackend("hdfs://localhost:9000/flink/checkpoints"));
        env.setRestartStrategy(RestartStrategies.noRestart());
        env.enableCheckpointing(1000L);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

//        version1(env);
        version2(env);
    }

    // 删除Filter算子
    private static void version1 (StreamExecutionEnvironment env) throws Exception {
        DataStream<String> source = env.socketTextStream("localhost", 9100, "\n")
                .name("MySourceFunction").uid("source-uid");
        DataStream<Tuple2<String, Integer>> wordsCount = source
                // 拆分
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String value, Collector out) {
                        for (String word : value.split("\\s")) {
                            out.collect(Tuple2.of(word.toUpperCase(), 1));
                        }
                    }
                }).name("wordsCountFlatMapFunction").uid("flatMap-uid")
                // 过滤
                .filter(new FilterFunction<Tuple2<String, Integer>>() {
                    @Override
                    public boolean filter(Tuple2<String, Integer> value) throws Exception {
                        String word = value.f0;
                        if (Objects.equals(word, "ERROR") || Objects.equals(word, "WARN")) {
                            return true;
                        }
                        return false;
                    }
                }).name("wordsCountFilterFunction").uid("filter-uid")
                // 分组
                .keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Integer> tuple) throws Exception {
                        return tuple.f0;
                    }
                })
                // 求和
                .sum(1).name("wordsCountSumFunction").uid("sum-uid");

        wordsCount.print().name("wordsCountPrintFunction");
        env.execute("TopologyChangesExample");
    }

    private static void version2 (StreamExecutionEnvironment env) throws Exception {
        DataStream<String> source = env.socketTextStream("localhost", 9100, "\n")
                .name("MySourceFunction").uid("source-uid");
        DataStream<Tuple2<String, Integer>> wordsCount = source
                // 拆分
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String value, Collector out) {
                        for (String word : value.split("\\s")) {
                            out.collect(Tuple2.of(word.toUpperCase(), 1));
                        }
                    }
                }).name("wordsCountFlatMapFunction").uid("flatMap-uid")
                // 分组
                .keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Integer> tuple) throws Exception {
                        return tuple.f0;
                    }
                })
                // 窗口
                .timeWindow(Time.hours(1), Time.seconds(5))
                // 求和
                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2 reduce(Tuple2<String, Integer> a, Tuple2<String, Integer> b) {
                        return new Tuple2(a.f0, a.f1 + b.f1);
                    }
                }).name("wordsCountReduceFunction").uid("reduce-uid");

        wordsCount.print().name("wordsCountPrintFunction");
        env.execute("TopologyChangesExample");
    }
}
