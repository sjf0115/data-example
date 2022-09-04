package com.flink.example.stream.watermark.custom;

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

/**
 * 功能：自定义实现 AscendingTimestampExtractor
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2022/9/4 下午5:29
 */
public abstract class CustomAscendingTimestampExtractor<T> implements AssignerWithPeriodicWatermarks<T> {
    private static final Logger LOG = LoggerFactory.getLogger(CustomAscendingTimestampExtractor.class);
    // 当前时间戳
    private long currentTimestamp = Long.MIN_VALUE;

    // 用户自定义实现时间戳提取逻辑
    public abstract long extractAscendingTimestamp(T element);

    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        long timestamp = Long.MIN_VALUE;
        if (currentTimestamp != Long.MIN_VALUE) {
            timestamp = currentTimestamp - 1;
        }
        LOG.info("current watermark: {}", timestamp);
        return new Watermark(timestamp);
    }

    @Override
    public long extractTimestamp(T element, long previousElementTimestamp) {
        // 提取时间戳
        final long newTimestamp = extractAscendingTimestamp(element);
        // 确保时间戳单调递增 新提取的时间戳要大于当前时间戳
        if (newTimestamp >= this.currentTimestamp) {
            this.currentTimestamp = newTimestamp;
        } else {
            LOG.warn("Timestamp monotony violated: {} < {}", newTimestamp, currentTimestamp);
        }
        LOG.info("current timestamp: {}", newTimestamp);
        return newTimestamp;
    }
}
