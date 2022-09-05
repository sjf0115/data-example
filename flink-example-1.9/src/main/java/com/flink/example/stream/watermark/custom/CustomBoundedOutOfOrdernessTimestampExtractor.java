package com.flink.example.stream.watermark.custom;

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 功能：自定义实现 BoundedOutOfOrdernessTimestampExtractor
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2022/9/5 上午8:09
 */
public abstract class CustomBoundedOutOfOrdernessTimestampExtractor<T> implements AssignerWithPeriodicWatermarks<T> {
    private static final Logger LOG = LoggerFactory.getLogger(CustomBoundedOutOfOrdernessTimestampExtractor.class);
    private static final long serialVersionUID = 1L;
    // 当前最大时间戳
    private long currentMaxTimestamp;
    // 当前 Watermark
    private long lastEmittedWatermark = Long.MIN_VALUE;
    // 最大乱序时间
    private final long maxOutOfOrderness;

    public CustomBoundedOutOfOrdernessTimestampExtractor(Time maxOutOfOrderness) {
        if (maxOutOfOrderness.toMilliseconds() < 0) {
            throw new RuntimeException("Tried to set the maximum allowed " + "lateness to " + maxOutOfOrderness + ". This parameter cannot be negative.");
        }
        this.maxOutOfOrderness = maxOutOfOrderness.toMilliseconds();
        this.currentMaxTimestamp = Long.MIN_VALUE + this.maxOutOfOrderness;
    }

    public long getMaxOutOfOrdernessInMillis() {
        return maxOutOfOrderness;
    }
    // 用户自定义实现时间戳提取逻辑
    public abstract long extractTimestamp(T element);

    @Override
    public final Watermark getCurrentWatermark() {
        // 保证 Watermark 递增的
        long potentialWM = currentMaxTimestamp - maxOutOfOrderness;
        if (potentialWM >= lastEmittedWatermark) {
            lastEmittedWatermark = potentialWM;
        }
        LOG.info("currentMaxTimestamp: {}, currentWatermark: {}", currentMaxTimestamp, lastEmittedWatermark);
        return new Watermark(lastEmittedWatermark);
    }

    @Override
    public final long extractTimestamp(T element, long previousElementTimestamp) {
        long timestamp = extractTimestamp(element);
        // 当前最大时间戳计算 Watermark
        if (timestamp > currentMaxTimestamp) {
            currentMaxTimestamp = timestamp;
        }
        return timestamp;
    }
}
