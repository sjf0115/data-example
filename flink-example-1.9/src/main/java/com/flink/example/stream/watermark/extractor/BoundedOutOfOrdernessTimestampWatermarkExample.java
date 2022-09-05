package com.flink.example.stream.watermark.extractor;

import com.common.example.utils.DateUtil;
import com.flink.example.stream.sink.PrintLogSinkFunction;
import com.flink.example.stream.source.simple.OutOfOrderSource;
import com.flink.example.stream.watermark.custom.CustomBoundedOutOfOrdernessTimestampExtractor;
import com.google.common.collect.Lists;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * 功能：周期性 Watermark 分配器 BoundedOutOfOrdernessTimestampExtractor 使用示例
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2022/8/31 下午11:30
 */
public class BoundedOutOfOrdernessTimestampWatermarkExample {
    private static final Logger LOG = LoggerFactory.getLogger(BoundedOutOfOrdernessTimestampWatermarkExample.class);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000L);
        // 设置事件时间特性
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // 每1s输出一次单词
        DataStreamSource<Tuple4<Integer, String, Integer, Long>> source = env.addSource(new OutOfOrderSource());
        // 计算每1分钟内每个单词出现的次数
        DataStream<Tuple3<String, Integer, String>> result = source
                .assignTimestampsAndWatermarks(new CustomBoundedOutOfOrdernessTimestampExtractor<Tuple4<Integer, String, Integer, Long>>(Time.seconds(5)) {
                    @Override
                    public long extractTimestamp(Tuple4<Integer, String, Integer, Long> element) {
                        return element.f3;
                    }
                })
                // 分组
                .keyBy(new KeySelector<Tuple4<Integer, String, Integer, Long>, String>() {
                    @Override
                    public String getKey(Tuple4<Integer, String, Integer, Long> element) throws Exception {
                        return element.f1;
                    }
                })
                // 每1分钟的滚动窗口
                .timeWindow(Time.minutes(1))
                // 求和
                .process(new ProcessWindowFunction<Tuple4<Integer, String, Integer, Long>, Tuple3<String, Integer, String>, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<Tuple4<Integer, String, Integer, Long>> elements, Collector<Tuple3<String, Integer, String>> out) throws Exception {
                        // 计算出现次数
                        int count = 0;
                        List<Integer> ids = Lists.newArrayList();
                        for (Tuple4<Integer, String, Integer, Long> element : elements) {
                            count += element.f2;
                            ids.add(element.f0);
                        }
                        // 当前 Watermark
                        long currentWatermark = context.currentWatermark();
                        // 时间窗口元数据
                        TimeWindow window = context.window();
                        long start = window.getStart();
                        long end = window.getEnd();
                        String startTime = DateUtil.timeStamp2Date(start, "yyyy-MM-dd HH:mm:ss");
                        String endTime = DateUtil.timeStamp2Date(end, "yyyy-MM-dd HH:mm:ss");
                        LOG.info("word: {}, count: {}, ids: {}, watermark: {}, windowStart: {}, windowEnd: {}",
                                key, count, ids.toString(), currentWatermark,
                                start + "|" + startTime, end + "|" + endTime
                        );
                        out.collect(Tuple3.of(key, count, ids.toString()));
                    }
                });

        // 输出并打印日志
        result.addSink(new PrintLogSinkFunction());
        env.execute("BoundedOutOfOrdernessTimestampWatermarkExample");
    }
}
