package com.flink.example.stream.join;

import com.common.example.utils.DateUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.time.Duration;

/**
 * 滑动窗口 Join Example
 * Created by wy on 2021/2/18.
 */
public class SlidingWindowJoinExample {
    private static final Logger LOG = LoggerFactory.getLogger(SlidingWindowJoinExample.class);
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置事件时间特性
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // Stream of (key, value, timestamp)
        DataStream<String> greenSource = env.socketTextStream("localhost", 9100, "\n");
        DataStream<String> orangeSource = env.socketTextStream("localhost", 9101, "\n");

        // 绿色流
        DataStream<Tuple3<String, String, String>> greenStream = greenSource.map(new MapFunction<String, Tuple3<String, String, String>>() {
            @Override
            public Tuple3<String, String, String> map(String str) throws Exception {
                String[] params = str.split(",");
                String key = params[0];
                String value = params[1];
                String eventTime = params[2];
                LOG.info("[绿色流] Key: {}, Value: {}, EventTime: {}", key, value, eventTime);
                return new Tuple3<>(key, value, eventTime);
            }
        }).assignTimestampsAndWatermarks(
                WatermarkStrategy.<Tuple3<String, String, String>>forBoundedOutOfOrderness(Duration.ofMillis(100))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, String>>() {
                            @Override
                            public long extractTimestamp(Tuple3<String, String, String> element, long recordTimestamp) {
                                Long timeStamp = 0L;
                                try {
                                    timeStamp = DateUtil.date2TimeStamp(element.f2, "yyyy-MM-dd HH:mm:ss");
                                } catch (ParseException e) {
                                    e.printStackTrace();
                                }
                                return timeStamp;
                            }
                        })
        );

        // 橘色流
        DataStream<Tuple3<String, String, String>> orangeStream = orangeSource.map(new MapFunction<String, Tuple3<String, String, String>>() {
            @Override
            public Tuple3<String, String, String> map(String str) throws Exception {
                String[] params = str.split(",");
                String key = params[0];
                String value = params[1];
                String eventTime = params[2];
                LOG.info("[橘色流] Key: {}, Value: {}, EventTime: {}", key, value, eventTime);
                return new Tuple3<>(key, value, eventTime);
            }
        }).assignTimestampsAndWatermarks(
                WatermarkStrategy.<Tuple3<String, String, String>>forBoundedOutOfOrderness(Duration.ofMillis(100))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, String>>() {
                            @Override
                            public long extractTimestamp(Tuple3<String, String, String> element, long recordTimestamp) {
                                Long timeStamp = 0L;
                                try {
                                    timeStamp = DateUtil.date2TimeStamp(element.f2, "yyyy-MM-dd HH:mm:ss");
                                } catch (ParseException e) {
                                    e.printStackTrace();
                                }
                                return timeStamp;
                            }
                        })
        );

        // 双流合并
        DataStream<String> result = orangeStream.join(greenStream)
                .where(tuple -> tuple.f0)
                .equalTo(tuple -> tuple.f0)
                .window(SlidingEventTimeWindows.of(Time.seconds(2) /* size */, Time.seconds(1) /* slide */))
                .apply(new JoinFunction<Tuple3<String, String, String>, Tuple3<String, String, String>, String>() {
                    @Override
                    public String join(Tuple3<String, String, String> first, Tuple3<String, String, String> second) throws Exception {
                        LOG.info("[合并流] Key: {}, Value: {}, EventTime: {}",
                                first.f0, first.f1 + "," + second.f1, first.f2 + "," + second.f2
                        );
                        return first.f1 + "," + second.f1;
                    }
                });

        result.print();

        env.execute("SlidingWindowJoinExample");
    }
//    绿色流：
//    key,0,2021-03-26 12:09:00
//    key,3,2021-03-26 12:09:03
//    key,4,2021-03-26 12:09:04
//    key,9,2021-03-26 12:09:09
//
//    橘色流：
//    key,0,2021-03-26 12:09:00
//    key,1,2021-03-26 12:09:01
//    key,2,2021-03-26 12:09:02
//    key,3,2021-03-26 12:09:03
//    key,4,2021-03-26 12:09:04
//    key,9,2021-03-26 12:09:09
}
