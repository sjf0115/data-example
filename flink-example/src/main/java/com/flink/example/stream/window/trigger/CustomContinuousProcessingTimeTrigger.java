package com.flink.example.stream.window.trigger;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 功能：自定义 ContinuousProcessingTimeTrigger
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2022/10/20 下午11:31
 */
public class CustomContinuousProcessingTimeTrigger<W extends Window> extends Trigger<Object, W> {

    private static final Logger LOG = LoggerFactory.getLogger(CustomContinuousProcessingTimeTrigger.class);

    private static final long serialVersionUID = 1L;
    // 触发周期间隔
    private final long interval;

    private final ReducingStateDescriptor<Long> stateDesc = new ReducingStateDescriptor<>(
            "fire-time", new Min(), LongSerializer.INSTANCE
    );

    private CustomContinuousProcessingTimeTrigger(long interval) {
        this.interval = interval;
    }

    @Override
    public TriggerResult onElement(Object element, long timestamp, W window, TriggerContext ctx) throws Exception {
        ReducingState<Long> fireTimestamp = ctx.getPartitionedState(stateDesc);
        timestamp = ctx.getCurrentProcessingTime();
        LOG.info("[OnElement] CurrentProcessingTime: {}", timestamp);
        // 注册处理时间定时器
        if (fireTimestamp.get() == null) {
            long start = timestamp - (timestamp % interval);
            long nextFireTimestamp = start + interval;
            LOG.info("[OnElement] 注册处理时间定时器 {}", nextFireTimestamp);
            ctx.registerProcessingTimeTimer(nextFireTimestamp);
            fireTimestamp.add(nextFireTimestamp);
            return TriggerResult.CONTINUE;
        }
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onEventTime(long time, W window, TriggerContext ctx) throws Exception {
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onProcessingTime(long time, W window, TriggerContext ctx) throws Exception {
        ReducingState<Long> fireTimestamp = ctx.getPartitionedState(stateDesc);
        Long fireTime = fireTimestamp.get();
        LOG.info("[OnProcessingTime]  time: {}, fireTime: {}", time, fireTime);
        // 判断是否当了触发器触发的时间点
        if (fireTime.equals(time)) {
            LOG.info("[OnProcessingTime] 触发计算");
            fireTimestamp.clear();
            fireTimestamp.add(time + interval);
            ctx.registerProcessingTimeTimer(time + interval);
            return TriggerResult.FIRE;
        }
        return TriggerResult.CONTINUE;
    }

    @Override
    public void clear(W window, TriggerContext ctx) throws Exception {
        // State could be merged into new window.
        ReducingState<Long> fireTimestamp = ctx.getPartitionedState(stateDesc);
        Long timestamp = fireTimestamp.get();
        if (timestamp != null) {
            ctx.deleteProcessingTimeTimer(timestamp);
            fireTimestamp.clear();
        }
    }

    @Override
    public boolean canMerge() {
        return true;
    }

    @Override
    public void onMerge(W window, OnMergeContext ctx) throws Exception {
        // States for old windows will lose after the call.
        ctx.mergePartitionedState(stateDesc);

        // Register timer for this new window.
        Long nextFireTimestamp = ctx.getPartitionedState(stateDesc).get();
        if (nextFireTimestamp != null) {
            ctx.registerProcessingTimeTimer(nextFireTimestamp);
        }
    }

    @VisibleForTesting
    public long getInterval() {
        return interval;
    }

    @Override
    public String toString() {
        return "ContinuousProcessingTimeTrigger(" + interval + ")";
    }

    // 创建 ContinuousProcessingTimeTrigger 触发器
    public static <W extends Window> CustomContinuousProcessingTimeTrigger<W> of(Time interval) {
        return new CustomContinuousProcessingTimeTrigger<>(interval.toMilliseconds());
    }

    private static class Min implements ReduceFunction<Long> {
        private static final long serialVersionUID = 1L;

        @Override
        public Long reduce(Long value1, Long value2) throws Exception {
            return Math.min(value1, value2);
        }
    }
}
