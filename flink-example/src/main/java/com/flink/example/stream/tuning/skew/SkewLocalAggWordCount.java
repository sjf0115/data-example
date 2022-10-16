package com.flink.example.stream.tuning.skew;

import com.flink.example.stream.source.simple.SkewWordMockSource;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

/**
 * 功能：数据倾斜版本 WordCount 实现本地聚合优化
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2022/10/14 下午11:30
 */
public class SkewLocalAggWordCount {
    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        // 设置状态后端
        env.setStateBackend(new HashMapStateBackend());

        // 设置Checkpoint存储
        env.enableCheckpointing(10000L);
        String checkpointPath = "hdfs://localhost:9000/flink/checkpoint";
        env.getCheckpointConfig().setCheckpointStorage(new FileSystemCheckpointStorage(checkpointPath));

        // 单词流 (word, 1)
        DataStreamSource<Tuple2<String, Integer>> source = env.addSource(new SkewWordMockSource());

        // 统计单词出现个数
        DataStream<Tuple2<String, Integer>> windowCount = source
                // 实现本地攒批聚合 每5个数据记录一个批次
                .process(new LocalAggProcessFunction(5))
                // 分组
                .keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Integer> tuple) throws Exception {
                        return tuple.f0;
                    }
                })
                // 求和
                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2 reduce(Tuple2<String, Integer> a, Tuple2<String, Integer> b) {
                        return new Tuple2(a.f0, a.f1 + b.f1);
                    }
                });

        // 输出
        windowCount.print();
        env.execute("SkewLocalAggWordCount");
    }

    // 本地攒批聚合
    private static class LocalAggProcessFunction extends ProcessFunction<Tuple2<String, Integer>, Tuple2<String, Integer>> {
        // 批次大小
        private int batchSize = 10;
        // 本地批次
        private transient Map<String, Integer> bundle;
        // 数据记录个数
        private transient int elements = 0;

        public LocalAggProcessFunction() {
        }

        public LocalAggProcessFunction(int batchSize) {
            this.batchSize = batchSize;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            this.bundle = new HashMap<>();
        }

        @Override
        public void processElement(Tuple2<String, Integer> element, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
            // 1. 将新到达的数据记录添加到批次中
            String word = element.f0;
            int count = element.f1;
            Integer bundleCount = bundle.getOrDefault(word, 0);
            bundle.put(word, bundleCount + count);
            // 处理的数据记录数
            elements ++;
            // 2. 当批次中的数据记录数达到批次大小则输出
            if (elements >= batchSize) {
                // 依次输出批次中的数据记录
                for (String bundleKey : bundle.keySet()) {
                    out.collect(Tuple2.of(bundleKey, bundle.get(bundleKey)));
                }
                // 批次清空
                bundle.clear();
                elements = 0;
            }
        }
    }
}
