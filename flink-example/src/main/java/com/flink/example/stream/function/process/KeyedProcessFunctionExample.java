package com.flink.example.stream.function.process;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/**
 * 功能：使用 KeyedProcessFunction 示例
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2022/9/13 上午9:33
 */
public class KeyedProcessFunctionExample {
    private static final Logger LOG = LoggerFactory.getLogger(KeyedProcessFunctionExample.class);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 输入源
        DataStream<String> source = env.socketTextStream("localhost", 9100, "\n");
        DataStream<Tuple2<String, Long>> stream = source.map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String str) throws Exception {
                String[] params = str.split(",");
                String key = params[0];
                Long timestamp = Long.parseLong(params[1]);
                LOG.info("输入元素 key: {}, timestamp: {}",  key, timestamp);
                return Tuple2.of(key, timestamp);
            }
        });

        // 逻辑处理
        DataStream<Tuple2<String, Long>> result = stream
                // 如果使用事件时间必须设置Timestamp提取和Watermark生成 否则下游 ctx.timestamp() 为null
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                            @Override
                            public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
                                return element.f1;
                            }
                        })
                )
                .keyBy(new KeySelector<Tuple2<String,Long>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Long> tuple) throws Exception {
                        return tuple.f0;
                    }
                })
                .process(new CounterKeyedProcessFunction());

        result.print();
        env.execute("KeyedProcessFunctionExample");
    }

    /**
     * 计数器 KeyedProcessFunction
     */
    private static class CounterKeyedProcessFunction extends KeyedProcessFunction<String, Tuple2<String, Long>, Tuple2<String, Long>> {
        // 状态
        private ValueState<EventState> state;
        @Override
        public void open(Configuration parameters) throws Exception {
            // 状态描述符
            ValueStateDescriptor<EventState> stateDescriptor = new ValueStateDescriptor<>("KeyedProcessFunctionState", EventState.class);
            // 状态
            state = getRuntimeContext().getState(stateDescriptor);
        }

        @Override
        public void processElement(Tuple2<String, Long> value, Context ctx, Collector<Tuple2<String, Long>> out) throws Exception {
            // 当前状态值
            EventState eventState = state.value();
            if (eventState == null) {
                eventState = new EventState();
                eventState.count = 0L;
                eventState.lastModified = Long.MIN_VALUE;
            }
            // 更新值
            eventState.count++;
            if (eventState.lastModified < ctx.timestamp()) {
                eventState.lastModified = ctx.timestamp();
            }
            // 更新状态
            state.update(eventState);

            String key = ctx.getCurrentKey();
            // 获取Watermark时间戳
            long watermark = ctx.timerService().currentWatermark();
            LOG.info("状态更新 key: {}, count: {}, lastModified: {}, Watermark: {}",
                    key, eventState.count, eventState.lastModified, watermark
            );

            // 注册事件时间定时器 10s 后触发
            Long registerEventTime = eventState.lastModified + 10*1000;
            ctx.timerService().registerEventTimeTimer(registerEventTime);
            LOG.info("注册事件时间定时器 key: {}, timer: {}", key, registerEventTime);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String, Long>> out) throws Exception {
            // timestamp 定时的事件时间
            // Key
            String key = ctx.getCurrentKey();
            // Watermark
            Long watermark = ctx.timerService().currentWatermark();
            // 检查这是一个过时的定时器还是最新的定时器
            EventState eventState = state.value();
            Long registerEventTime = eventState.lastModified + 10*1000; // 注册的事件时间定时器时间
            if (timestamp == registerEventTime) {
                // 最新注册的定时器 10s内没有更新输出当前值
                out.collect(Tuple2.of(key, eventState.count));
                LOG.info("最新定时器触发并输出 key: {}, count: {}, lastModified: {}, timer: {}, watermark: {}",
                        key, eventState.count, eventState.lastModified, timestamp, watermark
                );
            } else {
                // 过时的定时器
                LOG.info("过时定时器只触发不输出 key: {}, count: {}, lastModified: {}, timer: {}, watermark: {}",
                        key, eventState.count, eventState.lastModified, timestamp, watermark
                );
            }
        }
    }

    /**
     * 存储在状态中的数据结构
     */
    public static class EventState {
        public Long count;
        public Long lastModified;
    }
}

//a,1663071788000  -- 2022-09-13 20:23:08
//a,1663071792000  -- 2022-09-13 20:23:12
//a,1663071790000  -- 2022-09-13 20:23:10
//b,1663071803000  -- 2022-09-13 20:23:23
//c,1663071814000  -- 2022-09-13 20:23:34
//a,1663071825000  -- 2022-09-13 20:23:45
//b,1663071839000  -- 2022-09-13 20:23:59
//b,1663071901000  -- 2022-09-13 20:25:01