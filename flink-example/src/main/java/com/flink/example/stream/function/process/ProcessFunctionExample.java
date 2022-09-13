package com.flink.example.stream.function.process;

import com.common.example.utils.DateUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.time.Duration;

/**
 * ProcessFunction Example
 * Created by wy on 2021/2/28.
 */
public class ProcessFunctionExample {
    private static final Logger LOG = LoggerFactory.getLogger(ProcessFunctionExample.class);
    private static int delayTime = 10000;

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 输入源
        DataStream<String> source = env.socketTextStream("localhost", 9100, "\n");

        DataStream<Tuple2<String, String>> stream = source.map(new MapFunction<String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(String value) throws Exception {
                String[] params = value.split(",");
                String key = params[0];
                String eventTime = params[1];
                return new Tuple2<>(key, eventTime);
            }
        });

        // 事件时间戳以及watermark
        DataStream<Tuple2<String, Long>> result = stream.assignTimestampsAndWatermarks(
                // 最大乱序时间为5秒
                WatermarkStrategy.<Tuple2<String, String>>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, String>>() {
                            @Override
                            public long extractTimestamp(Tuple2<String, String> element, long recordTimestamp) {
                                Long timeStamp = 0L;
                                try {
                                    timeStamp = DateUtil.date2TimeStamp(element.f1);
                                } catch (ParseException e) {
                                    e.printStackTrace();
                                }
                                return timeStamp;
                            }
                        })
                )
                .keyBy(tuple -> tuple.f0)
                .process(new MyProcessFunction());

        result.print();
        env.execute("ProcessFunctionExample");
    }

    /**
     * 自定义ProcessFunction
     */
    private static class MyProcessFunction extends ProcessFunction<Tuple2<String, String>, Tuple2<String, Long>> {
        // 状态
        private ValueState<MyEvent> state;
        @Override
        public void open(Configuration parameters) throws Exception {
            // 状态描述符
            ValueStateDescriptor<MyEvent> stateDescriptor = new ValueStateDescriptor<>("ProcessFunctionState", MyEvent.class);
            // 状态
            state = getRuntimeContext().getState(stateDescriptor);
        }

        @Override
        public void processElement(Tuple2<String, String> value, Context ctx, Collector<Tuple2<String, Long>> out) throws Exception {
            // 获取Watermark时间戳
            long watermark = ctx.timerService().currentWatermark();
            LOG.info("[Watermark] watermark: [{}|{}]", watermark, DateUtil.timeStamp2Date(watermark));

            String key = value.f0;
            // 当前状态值
            MyEvent stateValue = state.value();
            if (stateValue == null) {
                stateValue = new MyEvent();
                stateValue.count = 0L;
            }
            // 更新值
            stateValue.key = key;
            stateValue.count++;
            stateValue.lastModified = ctx.timestamp();
            // 更新状态
            state.update(stateValue);

            // 注册事件时间定时器 10s后调用onTimer方法
            ctx.timerService().registerEventTimeTimer(stateValue.lastModified + delayTime);
            LOG.info("[Element] Key: {}, Count: {}, LastModified: [{}|{}]",
                    stateValue.key, stateValue.count, stateValue.lastModified,
                    DateUtil.timeStamp2Date(stateValue.lastModified)
            );
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String, Long>> out) throws Exception {
            // 当前状态值
            MyEvent stateValue = state.value();
            // 检查这是一个过时的定时器还是最新的定时器
            boolean isLatestTimer = false;
            if (timestamp == stateValue.lastModified + delayTime) {
                out.collect(new Tuple2<>(stateValue.key, stateValue.count));
                isLatestTimer = true;
            }

            Long timerTimestamp = ctx.timestamp();
            Long watermark = ctx.timerService().currentWatermark();
            LOG.info("[Timer] Key: {}, Count: {}, LastModified: [{}|{}], TimerTimestamp: [{}|{}], Watermark: [{}|{}], IsLatestTimer: {}",
                    stateValue.key, stateValue.count,
                    stateValue.lastModified, DateUtil.timeStamp2Date(stateValue.lastModified),
                    timerTimestamp, DateUtil.timeStamp2Date(timerTimestamp),
                    watermark, DateUtil.timeStamp2Date(watermark),
                    isLatestTimer
            );
        }
    }

    /**
     * 存储在状态中的数据结构
     */
    public static class MyEvent {
        public String key;
        public Long count;
        public Long lastModified;
    }
}

//a,2021-06-13 20:23:08
//a,2021-06-13 20:23:11
//b,2021-06-13 20:23:23
//c,2021-06-13 20:23:34
//a,2021-06-13 20:23:45
//b,2021-06-13 20:23:59
//b,2021-06-13 20:25:01