package com.flink.example.stream.window.assigner;

import com.flink.example.stream.source.simple.SimpleWordSource;
import com.flink.example.stream.watermark.CustomPeriodicWatermarkDeprecatedExample;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

/**
 * 功能：Count窗口示例
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2022/8/28 下午4:20
 */
public class CountWindowExample {
    private static final Logger LOG = LoggerFactory.getLogger(CustomPeriodicWatermarkDeprecatedExample.class);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置Checkpoint
        env.enableCheckpointing(1000L);
        // 设置事件时间特性
        //env.getConfig().setAutoWatermarkInterval(0);

        // Source 每1s输出一个单词
        DataStream<String> source = env.addSource(new SimpleWordSource());

        // Stream of (word, count)
        DataStream<Tuple2<String, Long>> words = source
                .flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
                    private Random random = new Random();
                    @Override
                    public void flatMap(String str, Collector<Tuple2<String, Long>> collector) throws Exception {
                        String[] words = str.split("\\s+");
                        for (String word : words) {
                            long count = random.nextInt(3);
                            LOG.info("[WordCount] {}:{}", word, count);
                            collector.collect(Tuple2.of(word, count));
                        }
                    }
                });

        // 滚动窗口 计算最近两个元素的和
        DataStream<Tuple2<String, Long>> tumblingStream = words
                // 根据单词分组
                .keyBy(new KeySelector<Tuple2<String,Long>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Long> tuple2) throws Exception {
                        return tuple2.f0;
                    }
                })
                // 2个元素大小的滚动计数窗口
                .countWindow(2)
                // 求和
                .sum(1);

        // 滑动窗口 每2个元素计算一次最近3个元素的和
        DataStream<Tuple2<String, Long>> slidingStream = words
                // 根据单词分组
                .keyBy(new KeySelector<Tuple2<String,Long>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Long> tuple2) throws Exception {
                        return tuple2.f0;
                    }
                })
                // 3个元素大小、步长为2个元素的滑动计数窗口
                .countWindow(3, 2)
                // 求和
                .sum(1);

        // 输出
        tumblingStream.print("tumbling");
        slidingStream.print("sliding");

        env.execute("CountWindowExample");
    }
}
