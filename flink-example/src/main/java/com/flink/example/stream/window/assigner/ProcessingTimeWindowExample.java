package com.flink.example.stream.window.assigner;

import com.flink.example.stream.source.simple.WordSource;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 功能：处理时间窗口示例
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2022/8/28 下午4:20
 */
public class ProcessingTimeWindowExample {
    private static final Logger LOG = LoggerFactory.getLogger(ProcessingTimeWindowExample.class);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置 Checkpoint
        env.enableCheckpointing(1000L);
        // 设置处理时间特性
        env.getConfig().setAutoWatermarkInterval(0);

        // 每次都递增1个 第一次输出1个单词 第二次输出两个单词 以此类推
        DataStream<String> source = env.addSource(new WordSource(30*1000, 4));

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

        // 滚动窗口 每1分钟统计每个单词的个数
        DataStream<Tuple2<String, Long>> tumblingTimeWindowStream = words
                // 根据单词分组
                .keyBy(new KeySelector<Tuple2<String,Long>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Long> tuple2) throws Exception {
                        return tuple2.f0;
                    }
                })
                // 窗口大小为1分钟的滚动窗口
                .window(TumblingProcessingTimeWindows.of(Time.minutes(1)))
                // 求和
                .sum(1);

        // 滑动窗口 每30s统计最近1分钟内的每个单词个数
        DataStream<Tuple2<String, Long>> slidingWindowStream = words
                // 根据单词分组
                .keyBy(new KeySelector<Tuple2<String,Long>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Long> tuple2) throws Exception {
                        return tuple2.f0;
                    }
                })
                // 窗口大小为1分钟、滑动步长为30秒的滑动窗口
                .window(SlidingProcessingTimeWindows.of(Time.minutes(1), Time.seconds(30)))
                // 求和
                .sum(1);

        // 输出
        tumblingTimeWindowStream.print("TumblingTimeWindow");
        slidingWindowStream.print("SlidingWindow");

        env.execute("ProcessingTimeWindowExample");
    }
}
