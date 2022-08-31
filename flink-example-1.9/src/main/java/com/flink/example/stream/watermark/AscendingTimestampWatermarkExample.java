package com.flink.example.stream.watermark;

import com.common.example.utils.DateUtil;
import com.flink.example.stream.source.simple.AscendingTimestampSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 功能：周期性 Watermark 分配器 AscendingTimestampExtractor 示例
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2022/8/31 下午11:32
 */
public class AscendingTimestampWatermarkExample {
    private static final Logger LOG = LoggerFactory.getLogger(AscendingTimestampWatermarkExample.class);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置 Checkpoint
        env.enableCheckpointing(1000L);
        // 设置事件时间特性
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // 每5s输出一次 Watermark
        env.getConfig().setAutoWatermarkInterval(5000);

        // 输入源 每2s输出一个单词
        DataStream<Tuple2<String, Long>> source = env.addSource(new AscendingTimestampSource(2000L));

        DataStream<Tuple2<String, Long>> stream = source
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple2<String, Long>>() {
                    @Override
                    public long extractAscendingTimestamp(Tuple2<String, Long> tuple) {
                        // 提取时间戳
                        return tuple.f1;
                    }
                })
                .map(new MapFunction<Tuple2<String,Long>, Tuple2<String,Long>>() {

                    @Override
                    public Tuple2<String, Long> map(Tuple2<String, Long> tuple2) throws Exception {
                        LOG.info("word: {}, timestamp: {}, time: {}", tuple2.f0, tuple2.f1, DateUtil.timeStamp2Date(tuple2.f1));
                        return Tuple2.of(tuple2.f0, 1L);
                    }
                })
                // 分组
                .keyBy(new KeySelector<Tuple2<String, Long>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Long> element) throws Exception {
                        return element.f0;
                    }
                })
                // 每10s一个窗口
                .timeWindow(Time.seconds(10))
                // 求和
                .sum(1);

        stream.print();
        env.execute("AscendingTimestampWatermarkExample");
    }
}
