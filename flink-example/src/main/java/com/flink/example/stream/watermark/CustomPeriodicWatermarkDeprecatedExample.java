package com.flink.example.stream.watermark;

import com.common.example.utils.DateUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

/**
 * PeriodicWatermark @Deprecated from 1.11
 * Created by wy on 2021/1/4.
 */
public class CustomPeriodicWatermarkDeprecatedExample {

    private static final Logger LOG = LoggerFactory.getLogger(CustomPeriodicWatermarkDeprecatedExample.class);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStateBackend((StateBackend) new FsStateBackend("hdfs://localhost:9000/flink/checkpoints"));
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,1000));
        env.enableCheckpointing(1000L);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // 设置事件时间特性
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<String> source = env.socketTextStream("localhost", 9100, "\n");

        DataStream<Tuple2<String, Long>> input = source
                .map(new MapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(String str) throws Exception {
                        String[] params = str.split(",");
                        String message = params[0];
                        String time = params[1];
                        Long timeStamp = DateUtil.date2TimeStamp(time, "yyyy-MM-dd HH:mm:ss");
                        return new Tuple2<>(message, timeStamp);
                    }
                });

        DataStream<Tuple2<String, Long>> result = input.assignTimestampsAndWatermarks(new CustomPeriodicWatermarkAssigner())
                // 转换
                .map(new MapFunction<Tuple2<String,Long>, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(Tuple2<String, Long> value) throws Exception {
                        return new Tuple2<String, Long>(value.f0, 1L);
                    }
                })
                // 分组
                .keyBy(new KeySelector<Tuple2<String, Long>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Long> tuple) throws Exception {
                        return tuple.f0;
                    }
                })
                // 窗口
                .timeWindow(Time.minutes(10), Time.minutes(5))
                // 求和
                .sum(1);

        result.print();
        env.execute("CustomPeriodicWatermarkDeprecatedExample");
    }

    public static class CustomPeriodicWatermarkAssigner implements AssignerWithPeriodicWatermarks<Tuple2<String, Long>> {

        private final Long outOfOrdernessMillis = 600000L;
        private Long currentMaxTimeStamp = Long.MIN_VALUE + outOfOrdernessMillis + 1;

        // 默认200ms被调用一次
        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(currentMaxTimeStamp - outOfOrdernessMillis - 1);
        }

        @Override
        public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {

            Watermark watermark = getCurrentWatermark();
            LOG.info("[INFO] watermark: {}", DateUtil.timeStamp2Date(watermark.getTimestamp(), "yyyy-MM-dd HH:mm:ss")+ " | " + watermark.getTimestamp() + "]");
            String key = element.f0;
            Long timestamp = element.f1;

            currentMaxTimeStamp = Math.max(timestamp, currentMaxTimeStamp);

            LOG.info("[INFO] timestamp, key: {}, eventTime: {}, currentMaxTimeStamp: {}, maxOutOfOrderness: {}",
                    key,
                    "[" + DateUtil.timeStamp2Date(timestamp, "yyyy-MM-dd HH:mm:ss") + " | " + timestamp + "]",
                    "[" + DateUtil.timeStamp2Date(currentMaxTimeStamp, "yyyy-MM-dd HH:mm:ss")+" | "+currentMaxTimeStamp + "]",
                    outOfOrdernessMillis
            );

            return timestamp;
        }
    }
}
