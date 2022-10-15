package com.flink.example.stream.tuning.localAgg;

import com.flink.example.stream.source.simple.SkewMockSource;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.util.Map;

/**
 * 功能：
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2022/10/15 下午9:43
 */
public class LocalAggProcessExample {
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
        DataStreamSource<Tuple2<String, Integer>> source = env.addSource(new SkewMockSource());

        // 统计单词出现个数
        DataStream<Tuple2<String, Integer>> windowCount = source
                .process(new LocalAggProcessFunction<String, Integer, Tuple2<String, Integer>, Tuple2<String, Integer>>(new CountBundleTrigger<>(5), new CountBundleFunction()) {
                    @Override
                    protected String getKey(Tuple2<String, Integer> element) throws Exception {
                        return element.f0;
                    }
                })
                .keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Integer> tuple) throws Exception {
                        return tuple.f0;
                    }
                })
                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2 reduce(Tuple2<String, Integer> a, Tuple2<String, Integer> b) {
                        return new Tuple2(a.f0, a.f1 + b.f1);
                    }
                });

        // 输出
        windowCount.print();
        env.execute("LocalAggProcessExample");
    }

    // 批次处理函数
    private static class CountBundleFunction extends BundleFunction<String,Integer,Tuple2<String,Integer>,Tuple2<String,Integer>> {
        // 往批次中新添加一个数据记录元素
        @Override
        public Integer addElement(@Nullable Integer count, Tuple2<String, Integer> element) throws Exception {
            return element.f1 + count;
        }

        // 批次结束时输出
        @Override
        public void finishBundle(Map<String, Integer> bundle, Collector<Tuple2<String, Integer>> out) throws Exception {
            // 依次输出批次中的数据记录
            for (String bundleKey : bundle.keySet()) {
                out.collect(Tuple2.of(bundleKey, bundle.get(bundleKey)));
            }
        }
    }
}
