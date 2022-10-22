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
 * 功能：自定义 ContinuousEventTimeTrigger
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2022/10/22 下午2:16
 */
public class CustomContinuousEventTimeTrigger<W extends Window> extends Trigger<Object, W> {
    private static final Logger LOG = LoggerFactory.getLogger(CustomContinuousEventTimeTrigger.class);
    private static final long serialVersionUID = 1L;
    private final long interval;
    private final ReducingStateDescriptor<Long> stateDesc =
            new ReducingStateDescriptor<>("fire-time", new Min(), LongSerializer.INSTANCE);

    private CustomContinuousEventTimeTrigger(long interval) {
        this.interval = interval;
    }

    @Override
    public TriggerResult onElement(Object element, long timestamp, W window, TriggerContext ctx) throws Exception {
        if (window.maxTimestamp() <= ctx.getCurrentWatermark()) {
            LOG.info("[OnElement] Watermark already past the window fire immediately, Watermark: {}, MaxTimestamp: {}", ctx.getCurrentWatermark(), window.maxTimestamp());
            // 如果 Watermark 已经超过了窗口结束时间 立即触发
            return TriggerResult.FIRE;
        } else {
            LOG.info("[OnElement] register eventTime timer, MaxTimestamp: {}", window.maxTimestamp());
            // 每个正常到达的元素都要注册事件时间定时器
            ctx.registerEventTimeTimer(window.maxTimestamp());
        }

        // 注册第一个事件时间定时器
        ReducingState<Long> fireTimestamp = ctx.getPartitionedState(stateDesc);
        if (fireTimestamp.get() == null) {
            long start = timestamp - (timestamp % interval);
            long nextFireTimestamp = start + interval;
            LOG.info("[OnElement] register eventTime timer, Timestamp: {}, NextFireTimestamp: {}", timestamp, nextFireTimestamp);
            ctx.registerEventTimeTimer(nextFireTimestamp);
            fireTimestamp.add(nextFireTimestamp);
        }
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onEventTime(long time, W window, TriggerContext ctx) throws Exception {
        // 当 Watermark 超过窗口最大时间 立即触发计算
        if (time == window.maxTimestamp()) {
            LOG.info("[OnEventTime] window fire, Time: {}, MaxTimestamp", time, window.maxTimestamp());
            return TriggerResult.FIRE;
        }
        // 否则判断是否是周期性触发
        ReducingState<Long> fireTimestampState = ctx.getPartitionedState(stateDesc);
        Long fireTimestamp = fireTimestampState.get();
        if (fireTimestamp != null && fireTimestamp == time) {
            long nextFireTimestamp = time + interval;
            LOG.info("[OnEventTime] window fire and register eventTime timer, Timestamp: {}, FireTimestamp:{}, NextFireTimestamp: {}", time, fireTimestamp, nextFireTimestamp);
            fireTimestampState.clear();
            fireTimestampState.add(nextFireTimestamp);
            ctx.registerEventTimeTimer(nextFireTimestamp);
            return TriggerResult.FIRE;
        }
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onProcessingTime(long time, W window, TriggerContext ctx) throws Exception {
        LOG.info("[OnProcessingTime] window continue, Time: {}", time);
        return TriggerResult.CONTINUE;
    }

    @Override
    public void clear(W window, TriggerContext ctx) throws Exception {
        ReducingState<Long> fireTimestamp = ctx.getPartitionedState(stateDesc);
        Long timestamp = fireTimestamp.get();
        if (timestamp != null) {
            LOG.info("[Clear] delete eventTime timer and clear fireTimestamp");
            ctx.deleteEventTimeTimer(timestamp);
            fireTimestamp.clear();
        }
    }

    @Override
    public boolean canMerge() {
        return true;
    }

    @Override
    public void onMerge(W window, OnMergeContext ctx) throws Exception {
        ctx.mergePartitionedState(stateDesc);
        Long nextFireTimestamp = ctx.getPartitionedState(stateDesc).get();
        if (nextFireTimestamp != null) {
            ctx.registerEventTimeTimer(nextFireTimestamp);
        }
    }

    @Override
    public String toString() {
        return "ContinuousEventTimeTrigger(" + interval + ")";
    }

    @VisibleForTesting
    public long getInterval() {
        return interval;
    }

    public static <W extends Window> CustomContinuousEventTimeTrigger<W> of(Time interval) {
        return new CustomContinuousEventTimeTrigger<>(interval.toMilliseconds());
    }

    private static class Min implements ReduceFunction<Long> {
        private static final long serialVersionUID = 1L;

        @Override
        public Long reduce(Long value1, Long value2) throws Exception {
            return Math.min(value1, value2);
        }
    }
}
