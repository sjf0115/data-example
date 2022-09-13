package com.flink.example.stream.watermark;

import com.common.example.utils.DateUtil;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 功能：自定义实现 断点式 Punctuated WatermarkStrategy
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2022/9/8 下午11:16
 */
public class CustomPunctuatedWatermarkStrategyExample {
    private static final Logger LOG = LoggerFactory.getLogger(CustomPunctuatedWatermarkStrategyExample.class);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> source = env.socketTextStream("localhost", 9100, "\n");
        DataStream<MyEvent> input = source.map(new MapFunction<String, MyEvent>() {
            @Override
            public MyEvent map(String str) throws Exception {
                String[] params = str.split(",");
                String key = params[0];
                Boolean hasWatermarkMarker = Boolean.parseBoolean(params[1]);
                String time = params[2];
                Long timestamp = DateUtil.date2TimeStamp(time, "yyyy-MM-dd HH:mm:ss");
                LOG.info("Key: {}, HashWatermark: {}, Timestamp: [{}|{}]",
                        key, hasWatermarkMarker, time, timestamp
                );
                return new MyEvent(key, hasWatermarkMarker, time, timestamp);
            }
        });

        // 提取时间戳、生成Watermark
        DataStream<MyEvent> watermarkStream = input.assignTimestampsAndWatermarks(new CustomWatermarkStrategy());
        // 也可以使用如下方式
        /*input.assignTimestampsAndWatermarks(new WatermarkStrategy<EventState>() {
            @Override
            public WatermarkGenerator<EventState> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                return new CustomPunctuatedGenerator();
            }
        }.withTimestampAssigner(new SerializableTimestampAssigner<EventState>() {
            @Override
            public long extractTimestamp(EventState element, long recordTimestamp) {
                return element.timestamp;
            }
        }));*/

        watermarkStream.print();

        env.execute("CustomPunctuatedWatermarkStrategyExample");
    }

    // 自定义 WatermarkStrategy
    public static class CustomWatermarkStrategy implements WatermarkStrategy<MyEvent> {
        // 创建 Watermark 生成器
        @Override
        public WatermarkGenerator<MyEvent> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
            return new CustomPunctuatedGenerator();
        }
        // 创建时间戳分配器
        @Override
        public TimestampAssigner<MyEvent> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
            return new CustomTimestampAssigner();
        }
    }

    // 自定义断点式 Watermark 生成器
    public static class CustomPunctuatedGenerator implements WatermarkGenerator<MyEvent> {
        @Override
        public void onEvent(MyEvent event, long eventTimestamp, WatermarkOutput output) {
            // 遇到特殊标记的元素就输出Watermark
            if (event.hasWatermarkMarker()) {
                Watermark watermark = new Watermark(eventTimestamp);
                LOG.info("Key: {}, HasWatermarkMarker: {}, EventTimestamp: [{}|{}], Watermark: [{}|{}]",
                        event.getKey(), event.hasWatermarkMarker(), event.getEventTime(), event.getTimestamp(),
                        watermark.getFormattedTimestamp(), watermark.getTimestamp()
                );
                output.emitWatermark(watermark);
            }
        }
        // 周期性生成 Watermark
        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            // 不需要
        }
    }

    // 自定义时间戳分配器
    public static class CustomTimestampAssigner implements TimestampAssigner<MyEvent> {
        @Override
        public long extractTimestamp(MyEvent element, long recordTimestamp) {
            return element.getTimestamp();
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

        @Override
        public String toString() {
            return "EventState{" +
                    "key='" + key + '\'' +
                    ", eventTime='" + eventTime + '\'' +
                    ", timestamp=" + timestamp +
                    ", hasWatermarkMarker=" + hasWatermarkMarker +
                    '}';
        }
    }
}
//A,false,2021-02-19 12:07:01
//B,true,2021-02-19 12:08:01
//A,false,2021-02-19 12:14:01
//C,false,2021-02-19 12:09:01
//C,true,2021-02-19 12:15:01
//A,true,2021-02-19 12:08:01