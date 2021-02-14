package com.flink.example.stream.window;

import com.flink.example.stream.watermark.CustomPeriodicWatermarkDeprecatedExample;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.shaded.curator4.com.google.common.base.Objects;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * AggregateFunction Example
 * Created by wy on 2021/2/12.
 */
public class AggregateFunctionExample {
    private static final Logger LOG = LoggerFactory.getLogger(CustomPeriodicWatermarkDeprecatedExample.class);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStateBackend((StateBackend) new FsStateBackend("hdfs://localhost:9000/flink/checkpoints"));
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,1000));
        env.enableCheckpointing(1000L);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // 设置事件时间特性
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        DataStream<String> source = env.socketTextStream("localhost", 9100, "\n");

        // Stream of (sensorId, isRunRedLight) eg: (A, true)
        DataStream<Tuple2<String, Integer>> sensor = source.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                String[] params = value.split(",");
                String sensorId = params[0];
                // 0表示闯红灯
                int isRunRedLight = 1;
                if (Objects.equal(params[1], "true")) {
                    isRunRedLight = 0;
                }
                LOG.info("Element: <{}, {}>", sensorId, isRunRedLight);
                return new Tuple2<String, Integer>(sensorId, isRunRedLight);
            }
        });

        // 求每个监测点摄像头最近一秒监控的有效通行率(=未闯红灯车次／总车次)
        DataStream<Double> result = sensor
                // 根据监测点分组
                .keyBy(0)
                // 窗口大小为1分钟的滚动窗口
                .timeWindow(Time.minutes(1))
                .aggregate(new AverageAggregateFunction());

        result.print();
        env.execute("AggregateFunctionExample");
    }

    /**
     * AggregateFunction
     */
    private static class AverageAggregateFunction implements AggregateFunction<Tuple2<String, Integer>, Tuple2<Long, Long>, Double> {

        // IN：Tuple2<String, Long>
        // ACC：Tuple2<Long, Long> -> <Sum, Count>
        // OUT：Double

        @Override
        public Tuple2<Long, Long> createAccumulator() {
            return new Tuple2<Long, Long>(0L, 0L);
        }

        @Override
        public Tuple2<Long, Long> add(Tuple2<String, Integer> value, Tuple2<Long, Long> accumulator) {
            return new Tuple2<Long, Long>(accumulator.f0 + value.f1, accumulator.f1 + 1L);
        }

        @Override
        public Double getResult(Tuple2<Long, Long> accumulator) {
            return ((double) accumulator.f0) / accumulator.f1;
        }

        @Override
        public Tuple2<Long, Long> merge(Tuple2<Long, Long> a, Tuple2<Long, Long> b) {
            return new Tuple2<Long, Long>(a.f0 + b.f0, a.f1 + b.f1);
        }
    }
}
