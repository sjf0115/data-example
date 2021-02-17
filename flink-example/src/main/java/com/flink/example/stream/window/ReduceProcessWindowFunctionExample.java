package com.flink.example.stream.window;

import com.common.example.utils.DateUtil;
import com.flink.example.bean.ContextInfo;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
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

/**
 * ReduceFunction增量函数与ProcessWindowFunction组合使用
 * Created by wy on 2021/2/15.
 */
public class ReduceProcessWindowFunctionExample {
    private static final Logger LOG = LoggerFactory.getLogger(ReduceProcessWindowFunctionExample.class);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStateBackend((StateBackend) new FsStateBackend("hdfs://localhost:9000/flink/checkpoints"));
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,1000));
        env.enableCheckpointing(1000L);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // 设置事件时间特性
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<String> source = env.socketTextStream("localhost", 9100, "\n");

        // Stream of (key, timestamp, 1)
        DataStream<Tuple3<String, Long, Integer>> stream = source.map(new MapFunction<String, Tuple3<String, Long, Integer>>() {
            @Override
            public Tuple3<String, Long, Integer> map(String str) throws Exception {
                String[] params = str.split(",");
                String key = params[0];
                String time = params[1];
                Long timeStamp = DateUtil.date2TimeStamp(time, "yyyy-MM-dd HH:mm:ss");
                LOG.info("[ELEMENT] Key: " + key + ", timeStamp: [" + timeStamp + "|" + time + "]");
                return new Tuple3(key, timeStamp, 1);
            }
        });

        // 滚动窗口
        DataStream<Tuple2<ContextInfo, Tuple2<String, Integer>>> result = stream
                // 提取时间戳与设置Watermark
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple3<String, Long, Integer>>(Time.minutes(10)) {
                    @Override
                    public long extractTimestamp(Tuple3<String, Long, Integer> element) {
                        return element.f1;
                    }
                })
                // 格式转换
                .map(new MapFunction<Tuple3<String,Long,Integer>, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(Tuple3<String, Long, Integer> value) throws Exception {
                        return new Tuple2<String, Integer>(value.f0, value.f2);
                    }
                })
                // 分组
                .keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Integer> value) throws Exception {
                        return value.f0;
                    }
                })
                // 窗口大小为10分钟、滑动步长为5分钟的滑动窗口
                .timeWindow(Time.minutes(10), Time.minutes(5))
                // ReduceFunction 相同单词将第二个字段求和
                .reduce(new MyReduceFunction(), new MyProcessWindowFunction());

        result.print();
        env.execute("ReduceProcessWindowFunctionExample");
    }

    /**
     * 自定义ReduceFunction：根据Key实现SUM
     */
    private static class MyReduceFunction implements ReduceFunction<Tuple2<String, Integer>> {
        public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) {
            return new Tuple2(value1.f0, value1.f1 + value2.f1);
        }
    }

    /**
     * 自定义ProcessWindowFunction：获取窗口元信息
     */
    private static class MyProcessWindowFunction extends ProcessWindowFunction<Tuple2<String, Integer>, Tuple2<ContextInfo, Tuple2<String, Integer>>, String, TimeWindow> {
        @Override
        public void process(String key, Context context, Iterable<Tuple2<String, Integer>> elements, Collector<Tuple2<ContextInfo, Tuple2<String, Integer>>> out) throws Exception {
            Tuple2<String, Integer> tuple = elements.iterator().next();
            // 窗口元信息
            TimeWindow window = context.window();
            long start = window.getStart();
            long end = window.getEnd();
            String startTime = DateUtil.timeStamp2Date(start, "yyyy-MM-dd HH:mm:ss");
            String endTime = DateUtil.timeStamp2Date(end, "yyyy-MM-dd HH:mm:ss");
            long currentWatermark = context.currentWatermark();
            String currentWatermarkTime = DateUtil.timeStamp2Date(currentWatermark, "yyyy-MM-dd HH:mm:ss");
            long currentProcessingTimeStamp = context.currentProcessingTime();
            String currentProcessingTime = DateUtil.timeStamp2Date(currentProcessingTimeStamp, "yyyy-MM-dd HH:mm:ss");

            ContextInfo contextInfo = new ContextInfo();
            contextInfo.setKey(tuple.f0);
            contextInfo.setSum(tuple.f1);
            contextInfo.setWindowStartTime(startTime);
            contextInfo.setWindowEndTime(endTime);
            contextInfo.setCurrentWatermark(currentWatermarkTime);
            contextInfo.setCurrentProcessingTime(currentProcessingTime);
            LOG.info("[WINDOW] " + contextInfo.toString());
            // 输出
            out.collect(new Tuple2<ContextInfo, Tuple2<String, Integer>>(contextInfo, tuple));
        }
    }
    // 输入样例
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
