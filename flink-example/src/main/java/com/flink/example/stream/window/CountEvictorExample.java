package com.flink.example.stream.window;

import com.common.example.utils.DateUtil;
import com.google.common.collect.Lists;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.evictors.CountEvictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;

/**
 * 功能：CountEvictor 示例
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2021/9/4 下午8:34
 */
public class CountEvictorExample {
    private static final Logger LOG = LoggerFactory.getLogger(CountEvictorExample.class);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> source = env.socketTextStream("localhost", 9100, "\n");

        // Stream of (key, value, time)
        DataStream<Tuple3<String, Long, Long>> stream = source
                .map(new MapFunction<String, Tuple3<String, Long, Long>>() {
                    @Override
                    public Tuple3<String, Long, Long> map(String str) throws Exception {
                        String[] params = str.split(",");
                        String key = params[0];
                        Long value = Long.parseLong(params[1]);
                        String eventTime = params[2];
                        Long timeStamp = DateUtil.date2TimeStamp(eventTime, "yyyy-MM-dd HH:mm:ss");
                        LOG.info("[元素] Key: {}, Value: {}, Time: [{}|{}]", key, value, eventTime, timeStamp);
                        return new Tuple3<>(key, value, timeStamp);
                    }
                })
                // 设置Watermark
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple3<String, Long, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, Long, Long>>() {
                                    @Override
                                    public long extractTimestamp(Tuple3<String, Long, Long> element, long recordTimestamp) {
                                        return element.f2;
                                    }
                                })
                );

        DataStream<Tuple2<String, Long>> result = stream
                // 格式转换
                .map(tuple -> Tuple2.of(tuple.f0, tuple.f1)).returns(Types.TUPLE(Types.STRING, Types.LONG))
                // 根据key分组
                .keyBy(new KeySelector<Tuple2<String, Long>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Long> value) throws Exception {
                        return value.f0;
                    }
                })
                // 处理时间滚动窗口 滚动大小60s
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                // 在触发使用窗口函数之前保留2个元素
                .evictor(CountEvictor.of(2))
                // 窗口函数
                .process(new ProcessWindowFunction<Tuple2<String, Long>, Tuple2<String, Long>, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<Tuple2<String, Long>> elements, Collector<Tuple2<String, Long>> out) throws Exception {
                        // Watermark
                        long watermark = context.currentWatermark();
                        String watermarkTime = DateUtil.timeStamp2Date(watermark);
                        // 窗口开始与结束时间
                        TimeWindow window = context.window();
                        String start = DateUtil.timeStamp2Date(window.getStart());
                        String end = DateUtil.timeStamp2Date(window.getEnd());
                        // 窗口中元素
                        List<Long> values = Lists.newArrayList();
                        for (Tuple2<String, Long> element : elements) {
                            values.add(element.f1);
                        }
                        LOG.info("[Process] Key: {}, Watermark: [{}|{}], Window: [{}|{}, {}|{}), Values: {}",
                                key, watermarkTime, watermark, start, window.getStart(), end, window.getEnd(), values
                        );
                    }
                });

        result.print();
        env.execute("CountEvictorExample");
    }
}
//    A,1,2021-08-30 12:07:20
//    A,2,2021-08-30 12:07:22
//    A,3,2021-08-30 12:07:33
//    A,4,2021-08-30 12:07:44
//    A,5,2021-08-30 12:07:55

//    A,6,2021-08-30 12:08:34
//    A,7,2021-08-30 12:08:45
//    A,8,2021-08-30 12:08:56
//    A,9,2021-08-30 12:09:30

//    A,10,2021-08-30 12:09:35