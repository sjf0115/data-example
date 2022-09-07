package com.flink.example.stream.window.late;

import com.common.example.utils.DateUtil;
import com.flink.example.stream.source.simple.OutOfOrderSource;
import com.google.common.collect.Lists;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;

/**
 * 功能：AllowedLateness 示例
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2022/9/4 上午9:55
 */
public class AllowedLatenessExample {
    private static final Logger LOG = LoggerFactory.getLogger(AllowedLatenessExample.class);

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 侧输出
        OutputTag<Tuple4<Integer, String, Integer, Long>> lateOutputTag = new OutputTag<Tuple4<Integer, String, Integer, Long>>("LATE"){};

        // 每1s输出一次单词
        DataStreamSource<Tuple4<Integer, String, Integer, Long>> source = env.addSource(new OutOfOrderSource());

        // 窗口计算
        SingleOutputStreamOperator<Tuple2<String, Integer>> stream = source
                .assignTimestampsAndWatermarks(
                        // 5s的最大乱序
                        WatermarkStrategy.<Tuple4<Integer, String, Integer, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple4<Integer, String, Integer, Long>>() {
                                    @Override
                                    public long extractTimestamp(Tuple4<Integer, String, Integer, Long> element, long recordTimestamp) {
                                        return element.f3;
                                    }
                                })
                )
                // 分组
                .keyBy(new KeySelector<Tuple4<Integer, String, Integer, Long>, String>() {
                    @Override
                    public String getKey(Tuple4<Integer, String, Integer, Long> tuple) throws Exception {
                        return tuple.f1;
                    }
                })
                // 1分钟的滚动窗口
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                // 最大允许延迟10s
                .allowedLateness(Time.seconds(10))
                // 迟到数据收集
                .sideOutputLateData(lateOutputTag)
                // 窗口计算
                .process(new ProcessWindowFunction<Tuple4<Integer, String, Integer, Long>, Tuple2<String, Integer>, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<Tuple4<Integer, String, Integer, Long>> elements, Collector<Tuple2<String, Integer>> out) throws Exception {
                        // 单词个数
                        int count = 0;
                        List<Integer> ids = Lists.newArrayList();
                        for (Tuple4<Integer, String, Integer, Long> element : elements) {
                            ids.add(element.f0);
                            count++;
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

        // 输出并打印日志
        stream.print("主链路");
        // 侧输出
        stream.getSideOutput(lateOutputTag).print("延迟链路");
        env.execute("AllowedLatenessExample");
    }
}
