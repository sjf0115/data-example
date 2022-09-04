package com.flink.example.stream.watermark.extractor;

import com.common.example.utils.DateUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 功能：周期性 Watermark 分配器 BoundedOutOfOrdernessTimestampExtractor 使用示例
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2022/8/31 下午11:30
 */
public class BoundedOutOfOrdernessTimestampWatermarkExample {
    private static final Logger LOG = LoggerFactory.getLogger(BoundedOutOfOrdernessTimestampWatermarkExample.class);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000L);

        // 设置事件时间特性
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // 每5s输出一次 Watermark
        env.getConfig().setAutoWatermarkInterval(5000);

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
        env.execute("BoundedOutOfOrdernessTimestampWatermarkExample");
    }
}
