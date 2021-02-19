package com.flink.example.stream.join;

import com.common.example.utils.DateUtil;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * WindowJoin Example
 * Created by wy on 2021/2/18.
 */
public class WindowJoinExample {
    private static final Logger LOG = LoggerFactory.getLogger(WindowJoinExample.class);
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置事件时间特性
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // Stream of (key, value, timestamp)
        // A流
        DataStream<String> aSource = env.socketTextStream("localhost", 9100, "\n");
        DataStream<String> bSource = env.socketTextStream("localhost", 9101, "\n");
        DataStream<Tuple3<String, String, Long>> aStream = aSource.map(new MapFunction<String, Tuple3<String, String, Long>>() {
            @Override
            public Tuple3<String, String, Long> map(String str) throws Exception {
                String[] params = str.split(",");
                String key = params[0];
                String value = params[1];
                String eventTime = params[2];
                Long timeStamp = DateUtil.date2TimeStamp(eventTime, "yyyy-MM-dd HH:mm:ss");
                LOG.info("[A流] Key: " + key + ", Value: " + value + ", TimeStamp: [" + eventTime + "|" + timeStamp + "]");
                return new Tuple3<>(key, value, timeStamp);
            }
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple3<String, String, Long>>(Time.seconds(10)) {
            @Override
            public long extractTimestamp(Tuple3<String, String, Long> element) {
                return element.f2;
            }
        });

        // B流
        DataStream<Tuple3<String, String, Long>> bStream = bSource.map(new MapFunction<String, Tuple3<String, String, Long>>() {
            @Override
            public Tuple3<String, String, Long> map(String str) throws Exception {
                String[] params = str.split(",");
                String key = params[0];
                String value = params[1];
                String eventTime = params[2];
                Long timeStamp = DateUtil.date2TimeStamp(eventTime, "yyyy-MM-dd HH:mm:ss");
                LOG.info("[B流] Key: " + key + ", Value: " + value + ", TimeStamp: [" + eventTime + "|" + timeStamp + "]");
                return new Tuple3<>(key, value, timeStamp);
            }
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple3<String, String, Long>>(Time.seconds(10)) {
            @Override
            public long extractTimestamp(Tuple3<String, String, Long> element) {
                return element.f2;
            }
        });

        // 双流合并
        DataStream<String> result = aStream.join(bStream)
                .where(tuple -> tuple.f0)
                .equalTo(tuple -> tuple.f0)
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .apply(new JoinFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, String>() {
                    @Override
                    public String join(Tuple3<String, String, Long> first, Tuple3<String, String, Long> second) throws Exception {
                        String result = "[AB合并流] Key: " + first.f0 + ", Value: " + first.f1 + "," + second.f1 + ", CurrentTime: " + DateUtil.currentDate();
                        LOG.info(result);
                        return result;
                    }
                });
        result.print();
        env.execute("WindowJoinExample");
    }
//    A流：
//    c,0,2021-02-19 12:09:01
//    c,1,2021-02-19 12:09:01
//    c,3,2021-02-19 12:21:02
//    c,4,2021-02-19 12:45:03
//    c,4,2021-02-19 13:39:03
//
//    B流：
//    c,0,2021-02-19 12:09:01
//    c,1,2021-02-19 12:09:01
//    c,2,2021-02-19 12:21:02
//    c,3,2021-02-19 12:21:02
//    c,4,2021-02-19 12:45:03
//    c,5,2021-02-19 12:45:03
//    c,6,2021-02-19 13:14:04
//    c,7,2021-02-19 13:14:04
//    c,4,2021-02-19 13:39:03
}
