package com.flink.example.stream.assigner;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 功能：基于处理时间的窗口
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2022/8/27 下午5:12
 */
public class ProcessingTimeWindowExample {
    private static final Logger LOG = LoggerFactory.getLogger(ProcessingTimeWindowExample.class);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置Checkpoint
        env.enableCheckpointing(1000L);
        // 设置事件时间特性 处理时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        DataStream<String> source = env.socketTextStream("localhost", 9100, "\n");

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

        // 滚动窗口 使用 timeWindow 方法
        DataStream<Tuple2<String, Long>> tumblingTimeWindowStream = words
                // 根据单词分组
                .keyBy(new KeySelector<Tuple2<String,Long>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Long> tuple2) throws Exception {
                        return tuple2.f0;
                    }
                })
                // 窗口大小为1分钟的滚动窗口
                .timeWindow(Time.minutes(1))
                // 求和
                .sum(1);

        // 滚动窗口 使用 window 方法
        DataStream<Tuple2<String, Long>> tumblingWindowStream = words
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

        // 滑动窗口 使用 timeWindow 方法
        DataStream<Tuple2<String, Long>> slidingTimeWindowStream = words
                // 根据单词分组
                .keyBy(new KeySelector<Tuple2<String,Long>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Long> tuple2) throws Exception {
                        return tuple2.f0;
                    }
                })
                // 窗口大小为1分钟、滑动步长为30秒的滑动窗口
                .timeWindow(Time.minutes(1), Time.seconds(30))
                // 求和
                .sum(1);

        // 滑动窗口 使用 window 方法
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
        tumblingWindowStream.print("TumblingWindow");
        slidingTimeWindowStream.print("SlidingTimeWindow");
        slidingWindowStream.print("SlidingWindow");

        env.execute("ProcessingTimeWindowExample");
    }
}
