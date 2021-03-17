package com.flink.example.stream.function;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ProcessFunction Example
 * Created by wy on 2021/2/28.
 */
public class ProcessFunctionExample {
    private static final Logger LOG = LoggerFactory.getLogger(KeyedProcessFunctionExample.class);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置事件时间特性
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStream<String> source = env.socketTextStream("localhost", 9100, "\n");

        DataStream<Long> result = source.map(new MapFunction<String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(String value) throws Exception {
                String[] params = value.split(",");
                String key = params[0];
                String eventTime = params[1];
                return new Tuple2<>(key, eventTime);
            }
        }).keyBy(tuple -> tuple.f0).process(new MyProcessFunction());

        result.print();
        env.execute("ProcessFunctionExample");
    }

    /**
     * 自定义ProcessFunction
     */
    private static class MyProcessFunction extends ProcessFunction<Tuple2<String, String>, Long> {

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
        public void processElement(Tuple2<String, String> value, Context ctx, Collector<Long> out) throws Exception {
            // 当前状态值
            MyEvent currentStateValue = state.value();
            if (currentStateValue == null) {
                currentStateValue = new MyEvent();
                currentStateValue.count = 0L;
            }

            // 更新值
            currentStateValue.count++;
            currentStateValue.lastModified = ctx.timestamp();

            // 更新状态
            state.update(currentStateValue);

            // 注册事件时间定时器 60s后调用onTimer方法
            ctx.timerService().registerEventTimeTimer(currentStateValue.lastModified + 60000);

            LOG.info("[ProcessElement] Count: {}, LastModified: {}",
                    currentStateValue.count, currentStateValue.lastModified
            );
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Long> out) throws Exception {
            // 当前状态值
            MyEvent currentStateValue = state.value();
            // 检查这是一个过时的定时器还是最新的定时器
            if (timestamp == currentStateValue.lastModified + 60000) {
                out.collect(currentStateValue.count);
            }
            LOG.info("[OnTimer] Count: {}, LastModified: {}, CurTimestamp: {}",
                    currentStateValue.count, currentStateValue.lastModified, timestamp
            );
        }
    }

    /**
     * 存储在状态中的数据结构
     */
    public static class MyEvent {
        public Long count;
        public Long lastModified;
    }

}
