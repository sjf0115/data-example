package com.flink.example.stream.watermark;

import com.common.example.utils.DateUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Periodic Watermark 1.11版本之前
 * Created by wy on 2021/1/24.
 */
public class PeriodicWatermarkDeprecatedExample {
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

        DataStream<Tuple3<String, Long, Integer>> input = source
                .map(new MapFunction<String, Tuple3<String, Long, Integer>>() {
                    @Override
                    public Tuple3<String, Long, Integer> map(String str) throws Exception {
                        LOG.info("[INFO] element: {}", str);
                        String[] params = str.split(",");
                        String key = params[0];
                        String time = params[1];
                        Long timeStamp = DateUtil.date2TimeStamp(time, "yyyy-MM-dd HH:mm:ss");
                        return new Tuple3(key, timeStamp, 1);
                    }
                });

        DataStream<Tuple3<String, Long, Integer>> result = input
//                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple3<String, Long, Integer>>() {
//                    @Override
//                    public long extractAscendingTimestamp(Tuple3<String, Long, Integer> tuple) {
//                        return tuple.f1;
//                    }
//                })
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple3<String, Long, Integer>>(Time.minutes(10)) {
                    @Override
                    public long extractTimestamp(Tuple3<String, Long, Integer> element) {
                        return element.f1;
                    }
                })
                // 分组
                .keyBy(new KeySelector<Tuple3<String, Long, Integer>, String>() {
                    @Override
                    public String getKey(Tuple3<String, Long, Integer> event) throws Exception {
                        return event.f0;
                    }
                })
                // 窗口
                .timeWindow(Time.minutes(10), Time.minutes(5))
                // 求和
                .sum(2);

        result.print();
        env.execute("PeriodicWatermarkDeprecatedExample");
    }
}
