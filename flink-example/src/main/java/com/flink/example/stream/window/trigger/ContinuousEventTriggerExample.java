package com.flink.example.stream.window.trigger;

import com.common.example.bean.LoginUser;
import com.flink.example.stream.connector.print.PrintLogSinkFunction;
import com.flink.example.stream.source.simple.UserLoginMockSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
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
public class ContinuousEventTriggerExample {
    private static final Logger LOG = LoggerFactory.getLogger(ContinuousEventTriggerExample.class);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<LoginUser> source = env.addSource(new UserLoginMockSource());

        SingleOutputStreamOperator<Tuple2<Integer, Integer>> result = source
                // 设置Watermark
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<LoginUser>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner(new SerializableTimestampAssigner<LoginUser>() {
                                    @Override
                                    public long extractTimestamp(LoginUser user, long recordTimestamp) {
                                        return user.getTimestamp();
                                    }
                                })
                )
                .map(new MapFunction<LoginUser, Tuple2<Integer, Integer>>() {
                    @Override
                    public Tuple2<Integer, Integer> map(LoginUser user) throws Exception {
                        return Tuple2.of(user.getAppId(), 1);
                    }
                })
                .keyBy(new KeySelector<Tuple2<Integer, Integer>, Integer>() {
                    @Override
                    public Integer getKey(Tuple2<Integer, Integer> user) throws Exception {
                        return user.f0;
                    }
                })
                // 事件时间滚动窗口 滚动大小60s
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                // 周期性事件时间触发器 每10s触发一次计算
                .trigger(CustomContinuousEventTimeTrigger.of(Time.seconds(10)))
                // 求和
                .reduce(new ReduceFunction<Tuple2<Integer, Integer>>() {
                    @Override
                    public Tuple2<Integer, Integer> reduce(Tuple2<Integer, Integer> value1, Tuple2<Integer, Integer> value2) throws Exception {
                        Integer result = value1.f1 + value2.f1;
                        return Tuple2.of(value1.f0, result);
                    }
                });

        // 打印日志并输出到控制台
        result.addSink(new PrintLogSinkFunction());
        env.execute("ContinuousEventTriggerExample");
    }
}

