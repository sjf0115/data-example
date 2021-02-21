package com.flink.example.stream.watermark;

import com.common.example.utils.DateUtil;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
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
        env.setStateBackend((StateBackend) new FsStateBackend("hdfs://localhost:9000/flink/checkpoints"));
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,1000));
        env.enableCheckpointing(1000L);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // 设置事件时间特性
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStream<String> source = env.socketTextStream("localhost", 9100, "\n");
        DataStream<MyEvent> input = source.map(new MapFunction<String, MyEvent>() {
            @Override
            public MyEvent map(String str) throws Exception {
                String[] params = str.split(",");
                String key = params[0];
                Boolean hasWatermarkMarker = Boolean.parseBoolean(params[1]);
                String time = DateUtil.currentDate();
                LOG.info("[INFO] Key: {}, HasWatermarkMarker: {}, TimeStamp: {}", key, hasWatermarkMarker, time);
                return new MyEvent(key, hasWatermarkMarker);
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
        );

        env.execute("PunctuatedWatermarkGeneratorExample");
    }

    /**
     * 自定义PunctuatedWatermarkGenerator
     */
    public static class MyPunctuatedWatermarkGenerator implements WatermarkGenerator<MyEvent> {

        @Override
        public void onEvent(MyEvent event, long eventTimestamp, WatermarkOutput output) {
            if (event.hasWatermarkMarker()) {
                Watermark watermark = new Watermark(event.getTimeStamp());
                LOG.info("[Window] TimeStamp: {}", watermark.getFormattedTimestamp());
                output.emitWatermark(watermark);
            }
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            // don't need to do anything because we emit in reaction to events above
        }
    }

    private static class MyEvent {
        private String key;
        private Long timeStamp;
        private Boolean hasWatermarkMarker;

        public MyEvent(String key, Boolean hasWatermarkMarker) {
            this.key = key;
            this.hasWatermarkMarker = hasWatermarkMarker;
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

        public Long getTimeStamp() {
            return timeStamp;
        }

        public void setTimeStamp(Long timeStamp) {
            this.timeStamp = timeStamp;
        }
    }
}
