package com.flink.example.stream.watermark;

import com.common.example.utils.DateUtil;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * PeriodicWatermarkGenerator 1.11版本开始
 * Created by wy on 2021/2/21.
 */
public class PeriodicWatermarkGeneratorExample {
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
        DataStream<Tuple3<String, Long, Integer>> input = source.map(new MapFunction<String, Tuple3<String, Long, Integer>>() {
                    @Override
                    public Tuple3<String, Long, Integer> map(String str) throws Exception {
                        String[] params = str.split(",");
                        String key = params[0];
                        String time = params[1];
                        Long timeStamp = DateUtil.date2TimeStamp(time, "yyyy-MM-dd HH:mm:ss");
                        LOG.info("[INFO] Key: {}, TimeStamp: [{}|{}]", key, time, timeStamp);
                        return new Tuple3<>(key, timeStamp, 1);
                    }
                });

        // 提取时间戳、生成Watermark
        DataStream<Tuple3<String, Long, Integer>> watermarkStream = input.assignTimestampsAndWatermarks(
                new WatermarkStrategy<Tuple3<String, Long, Integer>>() {
                    @Override
                    public WatermarkGenerator<Tuple3<String, Long, Integer>> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                        return new MyBoundedOutOfOrdernessGenerator();
                    }
                }
        );

        // 分组求和
        DataStream<Tuple2<String, Integer>> result = watermarkStream
                // 格式转换
                .map(tuple -> Tuple2.of(tuple.f0, tuple.f2)).returns(Types.TUPLE(Types.STRING, Types.INT))
                // 分组
                .keyBy(tuple -> tuple.f0)
                // 窗口大小为10分钟、滑动步长为5分钟的滑动窗口
                .timeWindow(Time.minutes(10), Time.minutes(5))
                // 分组求和
                .sum(1);

        result.print();
        env.execute("PeriodicWatermarkGeneratorExample");
    }

    /**
     * 自定义 BoundedOutOfOrdernessGenerator
     */
    private static class MyBoundedOutOfOrdernessGenerator implements WatermarkGenerator<Tuple3<String, Long, Integer>> {

        private final long maxOutOfOrderness = 3500; // 3.5 seconds

        private long currentMaxTimestamp;

        @Override
        public void onEvent(Tuple3<String, Long, Integer> event, long eventTimestamp, WatermarkOutput output) {
            currentMaxTimestamp = Math.max(currentMaxTimestamp, event.f1);
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            // emit the watermark as current highest timestamp minus the out-of-orderness bound
            output.emitWatermark(new Watermark(currentMaxTimestamp - maxOutOfOrderness - 1));
        }
    }

    /**
     * TimeLagWatermarkGenerator
     */
    public class TimeLagWatermarkGenerator implements WatermarkGenerator<Tuple3<String, Long, Integer>> {

        private final long maxTimeLag = 5000; // 5 seconds

        @Override
        public void onEvent(Tuple3<String, Long, Integer> event, long eventTimestamp, WatermarkOutput output) {
            // don't need to do anything because we work on processing time
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            output.emitWatermark(new Watermark(System.currentTimeMillis() - maxTimeLag));
        }
    }
}
