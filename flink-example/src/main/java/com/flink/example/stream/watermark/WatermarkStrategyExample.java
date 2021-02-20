package com.flink.example.stream.watermark;

import com.common.example.utils.DateUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/**
 * WatermarkStrategy Example
 * Created by wy on 2020/12/15.
 */
public class WatermarkStrategyExample {

    private static final Logger LOG = LoggerFactory.getLogger(WatermarkStrategyExample.class);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置事件时间特性
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 输入流
        DataStream<String> source = env.socketTextStream("localhost", 9100, "\n");
        DataStream<Tuple3<String, Long, Integer>> stream = source.map(new MapFunction<String, Tuple3<String, Long, Integer>>() {
            @Override
            public Tuple3<String, Long, Integer> map(String str) throws Exception {
                String[] params = str.split(",");
                String key = params[0];
                String eventTime = params[1];
                Long timeStamp = DateUtil.date2TimeStamp(eventTime, "yyyy-MM-dd HH:mm:ss");
                LOG.info("[输入元素] Key: {}, Timestamp: [{}|{}]", key, eventTime, timeStamp);
                return new Tuple3<String, Long, Integer>(key, timeStamp, 1);
            }
        });

        // 分配时间戳与设置Watermark
        SingleOutputStreamOperator<Tuple3<String, Long, Integer>> watermarkStream = stream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<Tuple3<String, Long, Integer>>forBoundedOutOfOrderness(Duration.ofMinutes(10))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, Long, Integer>>() {
                            @Override
                            public long extractTimestamp(Tuple3<String, Long, Integer> element, long recordTimestamp) {
                                return element.f1;
                            }
                        })
        );

        // 分组求和
        DataStream<Tuple2<String, Integer>> result = watermarkStream
                // 格式转换
                .map(tuple -> Tuple2.of(tuple.f0, tuple.f2)).returns(Types.TUPLE(Types.STRING, Types.INT))
                // 分组
                .keyBy(tuple -> tuple.f0)
                // 窗口大小为10分钟、滑动步长为5分钟的滑动窗口
                .timeWindow(Time.minutes(10), Time.minutes(5))
                // 分组求和
                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                        return new Tuple2(value1.f0, value1.f1 + value2.f1);
                    }
                });

        result.print();
        env.execute("WatermarkStrategyExample");
    }
//    // 输入样例
//    A,2021-02-19 12:07:01
//    B,2021-02-19 12:08:01
//    A,2021-02-19 12:14:01
//    C,2021-02-19 12:09:01
//    C,2021-02-19 12:15:01
//    A,2021-02-19 12:08:01
//    B,2021-02-19 12:13:01
//    B,2021-02-19 12:21:01
//    D,2021-02-19 12:04:01
//    B,2021-02-19 12:26:01
//    B,2021-02-19 12:17:01
//    D,2021-02-19 12:09:01
//    C,2021-02-19 12:30:01
}
