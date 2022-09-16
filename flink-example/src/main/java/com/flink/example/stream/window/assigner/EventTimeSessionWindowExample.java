package com.flink.example.stream.window.assigner;

import com.common.example.utils.DateUtil;
import com.google.common.collect.Lists;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;

/**
 * 功能：事件时间会话窗口示例
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2022/8/31 上午8:11
 */
public class EventTimeSessionWindowExample {
    private static final Logger LOG = LoggerFactory.getLogger(EventTimeSessionWindowExample.class);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // Source
        DataStream<Tuple4<Integer, String, Integer, Long>> source = env.addSource(new SourceFunction<Tuple4<Integer, String, Integer, Long>>() {
            private volatile boolean cancel = false;
            private List<Tuple4<Integer, String, Integer, Long>> elements = Lists.newArrayList(
                    // 歌曲Id, 用户Id, 听歌时长(分钟), 事件时间戳
                    Tuple4.of(1, "a", 2, 1663293601000L), // 10:00:01
                    Tuple4.of(2, "a", 5, 1663293920000L), // 10:05:20
                    Tuple4.of(3, "a", 3, 1663294133000L), // 10:08:53

                    Tuple4.of(4, "a", 2, 1663297374000L), // 11:02:54
                    Tuple4.of(5, "a", 6, 1663297746000L), // 11:09:06
                    Tuple4.of(6, "a", 5, 1663298643000L), // 11:24:03

                    Tuple4.of(7, "a", 3, 1663301601000L), // 12:13:21
                    Tuple4.of(8, "a", 1, 1663293773000L), // 10:02:53
                    Tuple4.of(9, "a", 1, 1663298757000L), // 11:25:57
                    Tuple4.of(10, "a", 3, 1663301682000L)  // 12:14:42
            );
            @Override
            public void run(SourceContext<Tuple4<Integer, String, Integer, Long>> ctx) throws Exception {
                int index = 0;
                while (!cancel && index < elements.size()) {
                    synchronized (ctx.getCheckpointLock()) {
                        Tuple4<Integer, String, Integer, Long> element = elements.get(index++);
                        LOG.info("id: {}, uid: {}, duration: {}, eventTime: {}|{}",
                                element.f0, element.f1, element.f2,
                                element.f3, DateUtil.timeStamp2Date(element.f3)
                        );
                        ctx.collect(element);
                    }
                    Thread.sleep(5*1000);
                }
            }

            @Override
            public void cancel() {
                cancel = true;
            }
        });

        // 会话窗口
        SingleOutputStreamOperator<String> sessionStream = source
                // 指定 Watermark 策略并分配时间戳 最大乱序时间10s
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple4<Integer, String, Integer, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple4<Integer, String, Integer, Long>>() {
                                    @Override
                                    public long extractTimestamp(Tuple4<Integer, String, Integer, Long> element, long recordTimestamp) {
                                        return element.f3;
                                    }
                                })
                )
                // 分区
                .keyBy(new KeySelector<Tuple4<Integer, String, Integer, Long>, String>() {
                    @Override
                    public String getKey(Tuple4<Integer, String, Integer, Long> element) throws Exception {
                        return element.f1;
                    }
                })
                // 会话窗口 15分钟一个会话
                .window(EventTimeSessionWindows.withGap(Time.minutes(15)))
                // 最大延迟一个小时
                .allowedLateness(Time.hours(1))
                // 求和
                .process(new ProcessWindowFunction<Tuple4<Integer, String, Integer, Long>, String, String, TimeWindow>() {
                    @Override
                    public void process(String uid, Context context, Iterable<Tuple4<Integer, String, Integer, Long>> elements, Collector<String> out) throws Exception {
                        int duration = 0;
                        List<Integer> songs = Lists.newArrayList();
                        for (Tuple4<Integer, String, Integer, Long> element : elements) {
                            duration += element.f2;
                            songs.add(element.f0);
                        }
                        // 时间窗口元数据
                        TimeWindow window = context.window();
                        long start = window.getStart();
                        long end = window.getEnd();
                        // 当前事件时间
                        long watermark = context.currentWatermark();
                        LOG.info("uid: {}, duration: {}分钟, songs: {}, window: {}, watermark: {}",
                                uid, duration, songs,
                                "[" + start + ", " + end + "]", watermark
                        );
                        out.collect("用户[" + uid + "]一次会话内听了" + duration + "分钟," + songs.size() + "首歌曲,歌曲列表为" + songs.toString());
                    }
                });
        // 输出
        sessionStream.print();
        env.execute("EventTimeSessionWindowExample");
    }
}
