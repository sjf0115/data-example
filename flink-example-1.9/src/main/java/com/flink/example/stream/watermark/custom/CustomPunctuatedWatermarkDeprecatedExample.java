package com.flink.example.stream.watermark.custom;

import com.common.example.utils.DateUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Objects;

/**
 * PunctuatedWatermark @Deprecated from 1.11
 * Created by wy on 2021/1/4.
 */
public class CustomPunctuatedWatermarkDeprecatedExample {

    private static final Logger LOG = LoggerFactory.getLogger(CustomPunctuatedWatermarkDeprecatedExample.class);

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
                        LOG.info("[INFO] element: [{}|{}]", message, time);
                        Long timeStamp = DateUtil.date2TimeStamp(time, "yyyy-MM-dd HH:mm:ss");
                        return new Tuple2<>(message, timeStamp);
                    }
                });

        DataStream<Tuple2<String, Long>> result = input.assignTimestampsAndWatermarks(
                new CustomPunctuatedWatermarkAssigner()
        );

        result.print();
        env.execute("CustomPunctuatedWatermarkDeprecatedExample");
    }

    /**
     * 自定义实现PunctuatedWatermark
     */
    public static class CustomPunctuatedWatermarkAssigner implements AssignerWithPunctuatedWatermarks<Tuple2<String, Long>> {
        @Nullable
        @Override
        public Watermark checkAndGetNextWatermark(Tuple2<String, Long> lastElement, long extractedTimestamp) {
            // 如果输入的元素为A时就会触发Watermark
            if (Objects.equals(lastElement.f0, "A")) {
                Watermark watermark = new Watermark(extractedTimestamp);
                LOG.info("[INFO] watermark: {}", DateUtil.timeStamp2Date(watermark.getTimestamp(), "yyyy-MM-dd HH:mm:ss") + " | " + watermark.getTimestamp() + "]");
                return watermark;
            }
            return null;
        }

        @Override
        public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
            // 抽取时间戳
            return element.f1;
        }
    }
}
