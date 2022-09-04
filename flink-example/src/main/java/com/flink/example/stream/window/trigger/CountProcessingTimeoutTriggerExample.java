package com.flink.example.stream.window.trigger;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 功能：带有超时时间的CountWindow
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2021/7/3 上午10:57
 */
public class CountProcessingTimeoutTriggerExample {
    private static final Logger LOG = LoggerFactory.getLogger(CountProcessingTimeoutTriggerExample.class);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000L);

        DataStream<String> source = env.socketTextStream("localhost", 9100, "\n");

        // Stream of (uid, duration)
        DataStream<Tuple2<String, Long>> stream = source
                .map(new MapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(String str) throws Exception {
                        String[] params = str.split(",");
                        String uid = params[0];
                        Long duration = Long.parseLong(params[1]);
                        LOG.info("[元素] uid: " + uid + ", duration: " + duration);
                        return new Tuple2<>(uid, duration);
                    }
                });

        // CountWindow
        DataStream<Tuple2<String, Long>> result = stream
                // 根据contentId分组
                .keyBy(new KeySelector<Tuple2<String, Long>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Long> value) throws Exception {
                        return value.f0;
                    }
                })
                // 全局窗口
                .window(GlobalWindows.create())
                // 每个窗口三个元素 1分钟没有元素到达触发超时
                .trigger(CountProcessingTimeoutTrigger.of(3L, 60000L))
                // 总时长
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> a, Tuple2<String, Long> b) throws Exception {
                        Long duration = a.f1 + b.f1;
                        LOG.info("[ReduceFunction] uid: {}, duration: {}", a.f0, duration);
                        return new Tuple2(a.f0, duration);
                    }
                });

        result.print();
        env.execute("CountProcessingTimeoutTriggerExample");
    }

    /**
     * 带有超时时间的 CountWindowTrigger
     */
    public static class CountProcessingTimeoutTrigger<T, W extends Window> extends Trigger<T, W> {

        private final long maxCount;
        private final long timeout;
        // 计数器状态
        private final ReducingStateDescriptor<Long> countStateDesc;
        // 超时时间状态
        private final ValueStateDescriptor<Long> timeoutStateDesc;

        private CountProcessingTimeoutTrigger(long maxCount, long timeout) {
            this.maxCount = maxCount;
            this.timeout = timeout;
            this.countStateDesc = new ReducingStateDescriptor<>("count", new SumReduceFunction(), LongSerializer.INSTANCE);
            this.timeoutStateDesc = new ValueStateDescriptor<>("timeout", LongSerializer.INSTANCE);
        }

        @Override
        public TriggerResult onElement(T element, long timestamp, W window, TriggerContext ctx) throws Exception {
            Tuple2<String, Long> tuple2 = (Tuple2<String, Long>)element;
            String uid = tuple2.f0;

            // 1. 计数器 超过最大阈值进行清除
            ReducingState<Long> countState = ctx.getPartitionedState(this.countStateDesc);
            countState.add(1L);
            if (countState.get() >= maxCount) {
                LOG.info("[OnElement] count of {} >= {}, trigger is fire and purge", uid, maxCount);
                this.clear(window, ctx);
                return TriggerResult.FIRE_AND_PURGE;
            }

            // 2. 注册处理时间定时器
            ValueState<Long> timeoutState = ctx.getPartitionedState(this.timeoutStateDesc);
            long nextFireTimestamp = ctx.getCurrentProcessingTime() + this.timeout;
            Long timeoutTimestamp = timeoutState.value();
            // 有新元素到来重新注册新定时器、删除老定时器
            if (timeoutTimestamp != null) {
                ctx.deleteProcessingTimeTimer(timeoutTimestamp);
                timeoutState.clear();
            }
            timeoutState.update(nextFireTimestamp);
            ctx.registerProcessingTimeTimer(nextFireTimestamp);
            LOG.info("[OnElement] register process time timer: [{}]", nextFireTimestamp);

            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onProcessingTime(long time, W window, TriggerContext ctx) throws Exception {
            // 处理时间定时器触发
            this.clear(window, ctx);
            LOG.info("[OnProcessingTime] process time timer is fire, trigger is fire and purge, currentProcessTime: {}", ctx.getCurrentProcessingTime());
            // FIRE_AND_PURGE 超时触发计算并清除数据
            // PURGE 超时不触发计算但清除数据
            return TriggerResult.FIRE_AND_PURGE;
        }

        @Override
        public TriggerResult onEventTime(long time, W window, TriggerContext ctx) throws Exception {
            // 不做触发
            return TriggerResult.CONTINUE;
        }

        @Override
        public void clear(W window, TriggerContext ctx) throws Exception {
            // 清除计数器状态
            ReducingState<Long> countState = ctx.getPartitionedState(countStateDesc);
            countState.clear();
            // 清除超时时间状态、删除处理时间定时器
            ValueState<Long> timeoutState = ctx.getPartitionedState(this.timeoutStateDesc);
            Long timeoutTimestamp = timeoutState.value();
            if (timeoutTimestamp != null) {
                ctx.deleteProcessingTimeTimer(timeoutTimestamp);
                timeoutState.clear();
            }
            LOG.info("[Trigger] clean state and delete process time timer");
        }

        public static <T, W extends Window> CountProcessingTimeoutTrigger<T, W> of(long maxCount, long timeout) {
            return new CountProcessingTimeoutTrigger<>(maxCount, timeout);
        }

        private static class SumReduceFunction implements ReduceFunction<Long> {
            private static final long serialVersionUID = 1L;

            @Override
            public Long reduce(Long value1, Long value2) throws Exception {
                return value1 + value2;
            }
        }
    }
}
// a,4
// a,2
// b,1
// b,4
// a,3
// b,5
// a,6
// b,3