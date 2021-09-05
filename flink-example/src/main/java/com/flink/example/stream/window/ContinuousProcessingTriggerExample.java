package com.flink.example.stream.window;

import com.common.example.utils.DateUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/**
 * 功能：周期性处理时间触发器
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2021/8/30 下午10:43
 */
public class ContinuousProcessingTriggerExample {
    private static final Logger LOG = LoggerFactory.getLogger(ContinuousProcessingTriggerExample.class);

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
                .window(TumblingProcessingTimeWindows.of(Time.minutes(1)))
                // 周期性处理时间触发器 每10s触发一次计算
                .trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(10)))
                // 求和
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                        Long result = value1.f1 + value2.f1;
                        LOG.info("[求和] Key: {}, Result: {} ({} + {})", value1.f0, result, value1.f1, value2.f1);
                        return new Tuple2(value1.f0, result);
                    }
                });

        result.print();
        env.execute("ContinuousProcessingTriggerExample");
    }
}
//    A,1,2021-08-30 12:07:01
//    B,2,2021-08-30 12:07:02
//    A,3,2021-08-30 12:07:11
//    B,4,2021-08-30 12:07:13
//    B,5,2021-08-30 12:07:24
//    A,6,2021-08-30 12:07:35
//    A,7,2021-08-30 12:07:46
//    B,8,2021-08-30 12:07:47
//    A,9,2021-08-30 12:07:58
//    B,10,2021-08-30 12:07:59
//    A,11,2021-08-30 12:08:03
//    B,12,2021-08-30 12:08:04
//    A,13,2021-08-30 12:08:14

