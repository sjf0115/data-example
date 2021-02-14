package com.flink.example.stream.window;

import com.common.example.utils.DateUtil;
import com.flink.example.stream.watermark.CustomPeriodicWatermarkDeprecatedExample;
import com.google.common.collect.Lists;
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
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * ProcessWindowFunction Example
 * 功能：分组求和
 * Created by wy on 2021/2/14.
 */
public class ProcessWindowFunctionExample {

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

        // Stream of (key, timestamp, value)
        DataStream<Tuple3<String, Long, Integer>> stream = source.map(new MapFunction<String, Tuple3<String, Long, Integer>>() {
            @Override
            public Tuple3<String, Long, Integer> map(String str) throws Exception {
                String[] params = str.split(",");
                String key = params[0];
                String time = params[1];
                Long timeStamp = DateUtil.date2TimeStamp(time, "yyyy-MM-dd HH:mm:ss");
                return new Tuple3(key, timeStamp, 1);
            }
        });

        DataStream result = stream
                // 提取时间戳与设置Watermark
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple3<String, Long, Integer>>(Time.minutes(10)) {
                    @Override
                    public long extractTimestamp(Tuple3<String, Long, Integer> element) {
                        return element.f1;
                    }
                })
                // 分组
                .keyBy(new KeySelector<Tuple3<String, Long, Integer>, String>() {

                    @Override
                    public String getKey(Tuple3<String, Long, Integer> value) throws Exception {
                        return value.f0;
                    }
                })
                // 窗口大小为10分钟、滑动步长为5分钟的滑动窗口
                .timeWindow(Time.minutes(10), Time.minutes(5))
                // 窗口函数
                .process(new MyProcessWindowFunction());

        result.print();
        env.execute("ProcessWindowFunctionExample");
    }

    /**
     * 自定义实现 ProcessWindowFunction
     */
    private static class MyProcessWindowFunction extends ProcessWindowFunction<Tuple3<String, Long, Integer>, String, String, TimeWindow> {
        @Override
        public void process(String key, Context context, Iterable<Tuple3<String, Long, Integer>> elements, Collector<String> out) throws Exception {
            long count = 0;
            List<String> list = Lists.newArrayList();
            for (Tuple3<String, Long, Integer> element : elements) {
                list.add(element.f0 + "|" + element.f1 + "|" + DateUtil.timeStamp2Date(element.f1, "yyyy-MM-dd HH:mm:ss"));
                Integer value = element.f2;
                count += value;
            }
            TimeWindow window = context.window();
            long start = window.getStart();
            long end = window.getEnd();
            String startTime = DateUtil.timeStamp2Date(start, "yyyy-MM-dd HH:mm:ss");
            String endTime = DateUtil.timeStamp2Date(end, "yyyy-MM-dd HH:mm:ss");
            long currentWatermark = context.currentWatermark();
            String currentWatermarkTime = DateUtil.timeStamp2Date(currentWatermark, "yyyy-MM-dd HH:mm:ss");
            long currentProcessingTimeStamp = context.currentProcessingTime();
            String currentProcessingTime = DateUtil.timeStamp2Date(currentProcessingTimeStamp, "yyyy-MM-dd HH:mm:ss");

            StringBuilder sb = new StringBuilder();
            sb.append("Key: " + list.toString());
            sb.append(", Window[" + startTime + ", " + endTime + "]");
            sb.append(", Count: " + count);
            sb.append(", CurrentWatermarkTime: " + currentWatermarkTime);
            sb.append(", CurrentProcessingTime: " + currentProcessingTime);
            out.collect(sb.toString());
        }
    }
//    输入样例：
//    A,2021-02-14 12:07:01
//    B,2021-02-14 12:08:01
//    A,2021-02-14 12:14:01
//    C,2021-02-14 12:09:01
//    C,2021-02-14 12:15:01
//    A,2021-02-14 12:08:01
//    B,2021-02-14 12:13:01
//    B,2021-02-14 12:21:01
//    D,2021-02-14 12:04:01
//    B,2021-02-14 12:26:01
//    B,2021-02-14 12:17:01
//    D,2021-02-14 12:09:01
//    C,2021-02-14 12:30:01

}
