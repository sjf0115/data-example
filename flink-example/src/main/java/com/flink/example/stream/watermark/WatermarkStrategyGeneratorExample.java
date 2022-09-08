package com.flink.example.stream.watermark;

import com.common.example.utils.DateUtil;
import com.flink.example.stream.source.simple.OutOfOrderSource;
import com.google.common.collect.Lists;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;

/**
 * 功能：使用 forGenerator 自定义实现 WatermarkGeneratorSupplier
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2022/9/8 下午11:16
 */
public class WatermarkStrategyGeneratorExample {
    private static final Logger LOG = LoggerFactory.getLogger(WatermarkStrategyExample.class);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 输入流
        DataStreamSource<Tuple4<Integer, String, Integer, Long>> source = env.addSource(new OutOfOrderSource());
        // 分组求和
        DataStream<Tuple2<String, Integer>> result = source
                // 使用自定义 WatermarkGeneratorSupplier
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.forGenerator(new CustomWatermarkGeneratorSupplier(Duration.ofSeconds(5)))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple4<Integer, String, Integer, Long>>() {
                            @Override
                            public long extractTimestamp(Tuple4<Integer, String, Integer, Long> element, long recordTimestamp) {
                                return element.f3;
                            }
                        })
                )
                // 分组
                .keyBy(new KeySelector<Tuple4<Integer,String,Integer,Long>, String>() {
                    @Override
                    public String getKey(Tuple4<Integer, String, Integer, Long> tuple) throws Exception {
                        return tuple.f1;
                    }
                })
                // 窗口大小为1分钟的滚动窗口
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                // 分组求和
                .process(new ProcessWindowFunction<Tuple4<Integer, String, Integer, Long>, Tuple2<String, Integer>, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<Tuple4<Integer, String, Integer, Long>> elements, Collector<Tuple2<String, Integer>> out) throws Exception {
                        // 单词个数
                        int count = 0;
                        List<Integer> ids = Lists.newArrayList();
                        for (Tuple4<Integer, String, Integer, Long> element : elements) {
                            ids.add(element.f0);
                            count += element.f2;
                        }
                        // 时间窗口元数据
                        TimeWindow window = context.window();
                        long start = window.getStart();
                        long end = window.getEnd();
                        String startTime = DateUtil.timeStamp2Date(start);
                        String endTime = DateUtil.timeStamp2Date(end);
                        // Watermark
                        long watermark = context.currentWatermark();
                        String watermarkTime = DateUtil.timeStamp2Date(watermark);
                        //  输出日志
                        LOG.info("word: {}, count: {}, ids: {}, window: {}, watermark: {}",
                                key, count, ids,
                                "[" + startTime + ", " + endTime + "]",
                                watermark + "|" + watermarkTime
                        );
                        out.collect(Tuple2.of(key, count));
                    }
                });

        result.print();
        env.execute("CustomWatermarkStrategyExample");
    }

    // 自定义 WatermarkGeneratorSupplier
    public static class CustomWatermarkGeneratorSupplier implements WatermarkGeneratorSupplier<Tuple4<Integer, String, Integer, Long>> {
        private final Duration maxOutOfOrderMillis;
        public CustomWatermarkGeneratorSupplier(Duration maxOutOfOrderMillis) {
            this.maxOutOfOrderMillis = maxOutOfOrderMillis;
        }
        // 创建 Watermark 生成器
        @Override
        public WatermarkGenerator<Tuple4<Integer, String, Integer, Long>> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
            return new CustomPeriodicGenerator(maxOutOfOrderMillis);
        }
    }

    // 自定义周期性 Watermark 生成器
    public static class CustomPeriodicGenerator implements WatermarkGenerator<Tuple4<Integer, String, Integer, Long>> {
        // 最大时间戳
        private long maxTimestamp;
        // 最大乱序时间
        private final long outOfOrderMillis;
        public CustomPeriodicGenerator(Duration maxOutOfOrderMillis) {
            this.outOfOrderMillis = maxOutOfOrderMillis.toMillis();
            // 起始最小 Watermark 为 Long.MIN_VALUE.
            this.maxTimestamp = Long.MIN_VALUE + outOfOrderMillis + 1;
        }
        // 最大时间戳
        @Override
        public void onEvent(Tuple4<Integer, String, Integer, Long> event, long eventTimestamp, WatermarkOutput output) {
            maxTimestamp = Math.max(maxTimestamp, event.f3);
        }
        // 周期性生成 Watermark
        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            output.emitWatermark(new Watermark(maxTimestamp - outOfOrderMillis - 1));
        }
    }
}
