package com.flink.example.stream.tuning.skew;

import com.flink.example.stream.source.simple.SkewWordMockSource;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 功能：数据倾斜版本 WordCount
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2022/10/14 下午11:30
 */
public class SkewWordCount {
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
        env.execute("SkewWordCount");
    }
}
