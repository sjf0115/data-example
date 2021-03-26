package com.flink.example.stream.join;

import com.common.example.utils.DateUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.time.Duration;

/**
 * Interval Join 示例
 * Created by wy on 2021/3/22.
 */
public class IntervalJoinExample {
    private static final Logger LOG = LoggerFactory.getLogger(TumblingWindowJoinExample.class);
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
                String eventTime = params[2];
                String value = params[1];
                LOG.info("[绿色流] Key: {}, Value: {}, EventTime: {}", key, value, eventTime);
                return new Tuple3<>(key, value, eventTime);
            }
        }).assignTimestampsAndWatermarks(
                WatermarkStrategy.<Tuple3<String, String, String>>forBoundedOutOfOrderness(Duration.ofMillis(100))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, String>>() {
                            @Override
                            public long extractTimestamp(Tuple3<String, String, String> element, long recordTimestamp) {
                                Long timeStamp = null;
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
                                Long timeStamp = null;
                                try {
                                    timeStamp = DateUtil.date2TimeStamp(element.f2, "yyyy-MM-dd HH:mm:ss");
                                } catch (ParseException e) {
                                    e.printStackTrace();
                                }
                                return timeStamp;
                            }
                        })
        );

        KeySelector<Tuple3<String, String, String>, String> keySelector = new KeySelector<Tuple3<String, String, String>, String>() {
            @Override
            public String getKey(Tuple3<String, String, String> value) throws Exception {
                return value.f0;
            }
        };

        // 双流合并
        DataStream result = orangeStream
                .keyBy(keySelector)
                .intervalJoin(greenStream.keyBy(keySelector))
                .between(Time.seconds(-2), Time.seconds(1))
                .process(new ProcessJoinFunction<Tuple3<String, String, String>, Tuple3<String, String, String>, String>() {
                    @Override
                    public void processElement(Tuple3<String, String, String> left,
                                               Tuple3<String, String, String> right,
                                               Context ctx, Collector<String> out) throws Exception {
                        LOG.info("[合并流] Key: {}, Value: {}, EventTime: {}",
                                left.f0, "[" + left.f1 + ", " + right.f1 + "]",
                                "[" + right.f2 + "|" + ctx.getRightTimestamp() + ", " + right.f2 + "|" + ctx.getLeftTimestamp() + "]"
                        );
                        out.collect(left.f1 + ", " + right.f1);
                    }
                });

        result.print();

        env.execute("IntervalJoinExample");
    }
//    绿色流：
//    c,0,2021-03-23 12:09:00
//    c,1,2021-03-23 12:09:01
//    c,6,2021-03-23 12:09:06
//    c,7,2021-03-23 12:09:07

//
//    橘色流：
//    c,0,2021-03-23 12:09:00
//    c,2,2021-03-23 12:09:02
//    c,3,2021-03-23 12:09:03
//    c,4,2021-03-23 12:09:04
//    c,5,2021-03-23 12:09:05
//    c,7,2021-03-23 12:09:07
}
