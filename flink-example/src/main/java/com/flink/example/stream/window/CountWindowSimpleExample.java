package com.flink.example.stream.window;

import com.flink.example.stream.watermark.CustomPeriodicWatermarkDeprecatedExample;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 计数窗口
 * Created by wy on 2021/1/31.
 */
public class CountWindowSimpleExample {
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

        // Stream of (sensorId, carCnt)
        DataStream<Tuple2<String, Long>> vehicleCnt = source
                .map(new MapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(String str) throws Exception {
                        String[] params = str.split(",");
                        String sensorId = params[0];
                        Long cnt = Long.parseLong(params[1]);
                        return new Tuple2<>(sensorId, cnt);
                    }
                });

        // 滚动窗口
        DataStream<Tuple2<String, Long>> tumblingCnt = vehicleCnt
                // 根据sensorId分组
                .keyBy(0)
                // 100个元素大小的滚动计数窗口
                .countWindow(100)
                // 求和
                .sum(1);

        // 滑动窗口
        DataStream<Tuple2<String, Long>> slidingCnt = vehicleCnt
                // 根据sensorId分组
                .keyBy(0)
                // 100个元素大小、步长为10个元素的滑动计数窗口
                .countWindow(100, 10)
                // 求和
                .sum(1);

        tumblingCnt.print();
        slidingCnt.print();
        env.execute("CountWindowSimpleExample");
    }
}
