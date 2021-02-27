package com.flink.example.stream.watermark;

import com.common.example.utils.DateUtil;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * PunctuatedWatermarkGenerator Example 1.11版本
 * Created by wy on 2021/2/21.
 */
public class PunctuatedWatermarkGeneratorExample {
    private static final Logger LOG = LoggerFactory.getLogger(PeriodicWatermarkGeneratorExample.class);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置事件时间特性
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStream<String> source = env.socketTextStream("localhost", 9100, "\n");
        DataStream<MyEvent> input = source.map(new MapFunction<String, MyEvent>() {
            @Override
            public MyEvent map(String str) throws Exception {
                String[] params = str.split(",");
                String key = params[0];
                Boolean hasWatermarkMarker = Boolean.parseBoolean(params[1]);
                String time = params[2];
                Long timestamp = DateUtil.date2TimeStamp(time, "yyyy-MM-dd HH:mm:ss");
                LOG.info("[Element] Key: {}, HashWatermark: {}, Timestamp: [{}|{}]",
                        key, hasWatermarkMarker, time, timestamp
                );
                return new MyEvent(key, hasWatermarkMarker, time, timestamp);
            }
        });

        // 提取时间戳、生成Watermark
        DataStream<MyEvent> watermarkStream = input.assignTimestampsAndWatermarks(
                new WatermarkStrategy<MyEvent>() {
                    @Override
                    public WatermarkGenerator<MyEvent> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                        return new MyPunctuatedWatermarkGenerator();
                    }
                }
                .withTimestampAssigner(new SerializableTimestampAssigner<MyEvent>() {
                    @Override
                    public long extractTimestamp(MyEvent element, long recordTimestamp) {
                        return element.getTimestamp();
                    }
                })
        );

        env.execute("PunctuatedWatermarkGeneratorExample");
    }

    /**
     * 自定义 Punctuated WatermarkGenerator
     */
    public static class MyPunctuatedWatermarkGenerator implements WatermarkGenerator<MyEvent> {

        @Override
        public void onEvent(MyEvent event, long eventTimestamp, WatermarkOutput output) {
            // 遇到特殊标记的元素就输出Watermark
            if (event.hasWatermarkMarker()) {
                Watermark watermark = new Watermark(eventTimestamp);
                LOG.info("[Watermark] Key: {}, HasWatermarkMarker: {}, EventTimestamp: [{}|{}], Watermark: [{}|{}]",
                        event.getKey(), event.hasWatermarkMarker(), event.getEventTime(), event.getTimestamp(),
                        watermark.getFormattedTimestamp(), watermark.getTimestamp()
                );
                output.emitWatermark(watermark);
            }
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            // 不使用该函数
        }
    }

    private static class MyEvent {
        private String key;
        private String eventTime;
        private Long timestamp;
        private Boolean hasWatermarkMarker;

        public MyEvent(String key, Boolean hasWatermarkMarker, String eventTime, Long timestamp) {
            this.key = key;
            this.hasWatermarkMarker = hasWatermarkMarker;
            this.eventTime = eventTime;
            this.timestamp = timestamp;
        }

        public String getEventTime() {
            return eventTime;
        }

        public void setEventTime(String eventTime) {
            this.eventTime = eventTime;
        }

        public String getKey() {
            return key;
        }

        public void setKey(String key) {
            this.key = key;
        }

        public Boolean hasWatermarkMarker() {
            return hasWatermarkMarker;
        }

        public void setWatermarkMarker(Boolean hasWatermarkMarker) {
            this.hasWatermarkMarker = hasWatermarkMarker;
        }

        public Long getTimestamp() {
            return timestamp;
        }

        public void setTimestamp(Long timestamp) {
            this.timestamp = timestamp;
        }
    }
}
