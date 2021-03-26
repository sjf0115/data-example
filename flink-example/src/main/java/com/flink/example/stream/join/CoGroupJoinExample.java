package com.flink.example.stream.join;

import com.common.example.utils.DateUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
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
 * CoGroup 实现内连接
 * Created by wy on 2021/3/24.
 */
public class CoGroupJoinExample {

    private static final Logger LOG = LoggerFactory.getLogger(CoGroupJoinExample.class);

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // A输入流
        DataStream<String> aSource = env.socketTextStream("localhost", 9100, "\n");
        // B输入流
        DataStream<String> bSource = env.socketTextStream("localhost", 9101, "\n");

        // 字段拆分
        MapFunction<String, Tuple3<String, String, String>> mapFunction = new MapFunction<String, Tuple3<String, String, String>>() {
            @Override
            public Tuple3<String, String, String> map(String str) throws Exception {
                String[] params = str.split(",");
                String key = params[0];
                String value = params[1];
                String eventTime = params[2];
                return Tuple3.of(key, value, eventTime);
            }
        };

        // Watermark
        WatermarkStrategy<Tuple3<String, String, String>> watermarkStrategy = WatermarkStrategy.<Tuple3<String, String, String>>forBoundedOutOfOrderness(Duration.ofSeconds(10))
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

        // A流
        DataStream<Tuple3<String, String, String>> aStream = aSource
                .map(mapFunction)
                .assignTimestampsAndWatermarks(watermarkStrategy);

        // B流
        DataStream<Tuple3<String, String, String>> bStream = bSource
                .map(mapFunction)
                .assignTimestampsAndWatermarks(watermarkStrategy);

        // Join流
        CoGroupedStreams<Tuple3<String, String, String>, Tuple3<String, String, String>> coGroupStream = aStream.coGroup(bStream);
        DataStream<String> result = coGroupStream
                // A流
                .where(new KeySelector<Tuple3<String, String, String>, String>() {
                    @Override
                    public String getKey(Tuple3<String, String, String> tuple3) throws Exception {
                        return tuple3.f0;
                    }
                })
                // B流
                .equalTo(new KeySelector<Tuple3<String, String, String>, String>() {
                    @Override
                    public String getKey(Tuple3<String, String, String> tuple3) throws Exception {
                        return tuple3.f0;
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .apply(new InnerJoinFunction());

        result.print();

        env.execute("CoGroupExample");
    }

    // 内连接
    private static class InnerJoinFunction implements CoGroupFunction<Tuple3<String, String, String>, Tuple3<String, String, String>, String> {
        @Override
        public void coGroup(Iterable<Tuple3<String, String, String>> iterableA, Iterable<Tuple3<String, String, String>> iterableB, Collector<String> collector) throws Exception {
            for (Tuple3<String, String, String> tupleA : iterableA) {
                for (Tuple3<String, String, String> tupleB : iterableB) {
                    LOG.info("[Join流] Key : {}, Value: {}",
                            tupleA.f0, "[" + tupleA.f1 + ", " + tupleB.f1 + "]", "[" + tupleA.f2 + ", " + tupleB.f2 + "]"
                    );
                    collector.collect( "Key: " + tupleA.f0 + ", Value: [" + tupleA.f1 + ", " + tupleB.f1 + "]");
                }
            }
        }
    }

    // 左连接
    private static class LeftJoinFunction implements CoGroupFunction<Tuple3<String, String, String>, Tuple3<String, String, String>, String> {
        @Override
        public void coGroup(Iterable<Tuple3<String, String, String>> iterableA, Iterable<Tuple3<String, String, String>> iterableB, Collector<String> collector) throws Exception {
            for (Tuple3<String, String, String> tupleA : iterableA) {
                boolean noElements = true;
                for (Tuple3<String, String, String> tupleB : iterableB) {
                    noElements = false;
                    LOG.info("[Join流] Key : {}, Value: {}",
                            tupleA.f0, "[" + tupleA.f1 + ", " + tupleB.f1 + "]", "[" + tupleA.f2 + ", " + tupleB.f2 + "]"
                    );
                    collector.collect( "Key: " + tupleA.f0 + ", Value: [" + tupleA.f1 + ", " + tupleB.f1 + "]");
                }
                if (noElements){
                    LOG.info("[Join流] Key : {}, Value: {}",
                            tupleA.f0, "[" + tupleA.f1 + ", null]", "[" + tupleA.f2 + ", null]"
                    );
                    collector.collect("Key: " + tupleA.f0 + ", Value: [" + tupleA.f1 + ", null]");
                }
            }
        }
    }

    // 右连接
    private static class RightJoinFunction implements CoGroupFunction<Tuple3<String, String, String>, Tuple3<String, String, String>, String> {
        @Override
        public void coGroup(Iterable<Tuple3<String, String, String>> iterableA, Iterable<Tuple3<String, String, String>> iterableB, Collector<String> collector) throws Exception {
            for (Tuple3<String, String, String> tupleB : iterableB) {
                boolean noElements = true;
                for (Tuple3<String, String, String> tupleA : iterableA) {
                    noElements = false;
                    LOG.info("[Join流] Key : {}, Value: {}",
                            tupleA.f0, "[" + tupleA.f1 + ", " + tupleB.f1 + "]", "[" + tupleA.f2 + ", " + tupleB.f2 + "]"
                    );
                    collector.collect( "Key: " + tupleA.f0 + ", Value: [" + tupleA.f1 + ", " + tupleB.f1 + "]");
                }
                if (noElements){
                    LOG.info("[Join流] Key : {}, Value: {}",
                            tupleB.f0, "[null, " + tupleB.f1 + "]", "[null, " + tupleB.f2 + "]"
                    );
                    collector.collect("Key: " + tupleB.f0 + ", Value: [null, " + tupleB.f2 + "]");
                }
            }
        }
    }
}
