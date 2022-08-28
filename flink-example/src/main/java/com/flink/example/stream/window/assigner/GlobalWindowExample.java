package com.flink.example.stream.window.assigner;

import com.flink.example.stream.source.simple.SimpleWordSource;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.util.Collector;

/**
 * 功能：GlobalWindow 示例
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2022/8/28 下午4:20
 */
public class GlobalWindowExample {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置Checkpoint
        env.enableCheckpointing(1000L);
        // 处理时间
        env.getConfig().setAutoWatermarkInterval(0);
        // Source 每1s输出一个单词
        DataStream<String> source = env.addSource(new SimpleWordSource());

        // Stream of (word, count)
        DataStream<Tuple2<String, Long>> words = source
                .flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public void flatMap(String str, Collector<Tuple2<String, Long>> collector) throws Exception {
                        String[] words = str.split("\\s+");
                        for (String word : words) {
                            collector.collect(Tuple2.of(word, 1L));
                        }
                    }
                });

        // 全局窗口
        DataStream<Tuple2<String, Long>> stream = words
                // 根据单词分组
                .keyBy(new KeySelector<Tuple2<String,Long>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Long> tuple2) throws Exception {
                        return tuple2.f0;
                    }
                })
                // 全局窗口
                .window(GlobalWindows.create())
                // 分组内每两个元素触发一次输出
                .trigger(CountTrigger.of(2))
                // 求和
                .sum(1);

        // 输出
        stream.print();
        env.execute("GlobalWindowExample");
    }
}
