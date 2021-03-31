package com.flink.example.stream.join;

import com.common.example.utils.DateUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.CoGroupedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.time.Duration;

/**
 * 功能：CoGroup 实现内连接
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2021/3/28 下午5:56
 */
public class CoGroupJoinExample {

    private static final Logger LOG = LoggerFactory.getLogger(CoGroupJoinExample.class);

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置事件时间特性
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 输入流
        DataStream<String> greenSource = env.socketTextStream("localhost", 9100, "\n");
        DataStream<String> orangeSource = env.socketTextStream("localhost", 9101, "\n");

        // Watermark
        WatermarkStrategy<Tuple3<String, String, String>> watermarkStrategy = WatermarkStrategy.<Tuple3<String, String, String>>forBoundedOutOfOrderness(Duration.ofMillis(100))
                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, String>>() {
                    @Override
                    public long extractTimestamp(Tuple3<String, String, String> element, long recordTimestamp) {
                        Long timeStamp = null;
                        try {
                            timeStamp = DateUtil.date2TimeStamp(element.f2, "yyyy-MM-dd HH:mm:ss");
                        } catch (ParseException e) {
                            e.printStackTrace();
                        }
                        return timeStamp;
                    }
                });

        // 绿色流
        DataStream<Tuple3<String, String, String>> greenStream = greenSource
                .map(new MapFunction<String, Tuple3<String, String, String>>() {
                    @Override
                    public Tuple3<String, String, String> map(String str) throws Exception {
                        String[] params = str.split(",");
                        String key = params[0];
                        String value = params[1];
                        String eventTime = params[2];
                        LOG.info("[绿色流] Key: {}, Value: {}, EventTime: {}", key, value, eventTime);
                        return Tuple3.of(key, value, eventTime);
                    }
                })
                .assignTimestampsAndWatermarks(watermarkStrategy);

        // 橘色流
        DataStream<Tuple3<String, String, String>> orangeStream = orangeSource
                .map(new MapFunction<String, Tuple3<String, String, String>>() {
                    @Override
                    public Tuple3<String, String, String> map(String str) throws Exception {
                        String[] params = str.split(",");
                        String key = params[0];
                        String value = params[1];
                        String eventTime = params[2];
                        LOG.info("[橘色流] Key: {}, Value: {}, EventTime: {}", key, value, eventTime);
                        return Tuple3.of(key, value, eventTime);
                    }
                })
                .assignTimestampsAndWatermarks(watermarkStrategy);

        // Join流
        CoGroupedStreams coGroupStream = greenStream.coGroup(orangeStream);
        DataStream<String> result = coGroupStream
                // 绿色流
                .where(new KeySelector<Tuple3<String, String, String>, String>() {
                    @Override
                    public String getKey(Tuple3<String, String, String> tuple3) throws Exception {
                        return tuple3.f0;
                    }
                })
                // 橘色流
                .equalTo(new KeySelector<Tuple3<String, String, String>, String>() {
                    @Override
                    public String getKey(Tuple3<String, String, String> tuple3) throws Exception {
                        return tuple3.f0;
                    }
                })
                // 滚动窗口
                .window(TumblingEventTimeWindows.of(Time.seconds(2)))
                .apply(new InnerJoinFunction());

        result.print();

        env.execute("CoGroupExample");
    }

    // 内连接
    private static class InnerJoinFunction implements CoGroupFunction<Tuple3<String, String, String>, Tuple3<String, String, String>, String> {
        @Override
        public void coGroup(Iterable<Tuple3<String, String, String>> greenIterable, Iterable<Tuple3<String, String, String>> orangeIterable, Collector<String> collector) throws Exception {
            for (Tuple3<String, String, String> greenTuple : greenIterable) {
                for (Tuple3<String, String, String> orangeTuple : orangeIterable) {
                    LOG.info("[Join流] Key : {}, Value: {}, EventTime: {}",
                            greenTuple.f0, greenTuple.f1 + ", " + orangeTuple.f1, greenTuple.f2 + ", " + orangeTuple.f2
                    );
                    collector.collect(greenTuple.f1 + ", " + orangeTuple.f1);
                }
            }
        }
    }

    // 左连接
    private static class LeftJoinFunction implements CoGroupFunction<Tuple3<String, String, String>, Tuple3<String, String, String>, String> {
        @Override
        public void coGroup(Iterable<Tuple3<String, String, String>> greenIterable, Iterable<Tuple3<String, String, String>> orangeIterable, Collector<String> collector) throws Exception {
            for (Tuple3<String, String, String> greenTuple : greenIterable) {
                boolean noElements = true;
                for (Tuple3<String, String, String> orangeTuple : orangeIterable) {
                    noElements = false;
                    LOG.info("[Join流] Key : {}, Value: {}, EventTime: {}",
                            greenTuple.f0, greenTuple.f1 + ", " + orangeTuple.f1, greenTuple.f2 + ", " + orangeTuple.f2
                    );
                    collector.collect(greenTuple.f1 + ", " + orangeTuple.f1);
                }
                if (noElements){
                    LOG.info("[Join流] Key : {}, Value: {}, EventTime: {}",
                            greenTuple.f0, greenTuple.f1 + ", null", greenTuple.f2 + ", null"
                    );
                    collector.collect(greenTuple.f1 + ", null");
                }
            }
        }
    }

    // 右连接
    private static class RightJoinFunction implements CoGroupFunction<Tuple3<String, String, String>, Tuple3<String, String, String>, String> {
        @Override
        public void coGroup(Iterable<Tuple3<String, String, String>> greenIterable, Iterable<Tuple3<String, String, String>> orangeIterable, Collector<String> collector) throws Exception {
            for (Tuple3<String, String, String> orangeTuple : orangeIterable) {
                boolean noElements = true;
                for (Tuple3<String, String, String> greenTuple : greenIterable) {
                    noElements = false;
                    LOG.info("[Join流] Key : {}, Value: {}, EventTime: {}",
                            greenTuple.f0, greenTuple.f1 + ", " + orangeTuple.f1, greenTuple.f2 + ", " + orangeTuple.f2
                    );
                    collector.collect(greenTuple.f1 + ", " + orangeTuple.f1);
                }
                if (noElements) {
                    LOG.info("[Join流] Key : {}, Value: {}, EventTime: {}",
                            orangeTuple.f0, "null, " + orangeTuple.f1, "null, " + orangeTuple.f2
                    );
                    collector.collect("null, " + orangeTuple.f2);
                }
            }
        }
    }
//    绿色流：
//    key,0,2021-03-26 12:09:00
//    key,1,2021-03-26 12:09:01
//    key,2,2021-03-26 12:09:02
//    key,4,2021-03-26 12:09:04
//    key,5,2021-03-26 12:09:05
//    key,8,2021-03-26 12:09:08
//    key,9,2021-03-26 12:09:09
//    key,11,2021-03-26 12:09:11
//
//    橘色流：
//    key,0,2021-03-26 12:09:00
//    key,1,2021-03-26 12:09:01
//    key,2,2021-03-26 12:09:02
//    key,3,2021-03-26 12:09:03
//    key,4,2021-03-26 12:09:04
//    key,6,2021-03-26 12:09:06
//    key,7,2021-03-26 12:09:07
//    key,11,2021-03-26 12:09:11
}
