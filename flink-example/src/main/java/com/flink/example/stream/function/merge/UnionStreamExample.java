package com.flink.example.stream.function.merge;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/**
 * 功能：使用 Union 合并流
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2022/9/13 上午9:33
 */
public class UnionStreamExample {
    private static final Logger LOG = LoggerFactory.getLogger(UnionStreamExample.class);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // A 输入流
        SingleOutputStreamOperator<Tuple3<String, String, Long>> aStream = env.socketTextStream("localhost", 9100, "\n")
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner(new SerializableTimestampAssigner<String>() {
                                    @Override
                                    public long extractTimestamp(String element, long recordTimestamp) {
                                        String[] params = element.split(",");
                                        return Long.parseLong(params[1]);
                                    }
                                })
                )
                // 添加 ProcessFunction 用于输出 A 流的 Watermark
                .process(new ProcessFunction<String, Tuple3<String, String, Long>>() {
                    @Override
                    public void processElement(String element, Context ctx, Collector<Tuple3<String, String, Long>> out) throws Exception {
                        String[] params = element.split(",");
                        String word = params[0];
                        Long timestamp = Long.parseLong(params[1]);
                        Long watermark = ctx.timerService().currentWatermark();
                        LOG.info("AStream word: {}, timestamp: {}, watermark: {}", word, timestamp, watermark);
                        out.collect(Tuple3.of("AStream", word, timestamp));
                    }
                });

        // B 输入流
        SingleOutputStreamOperator<Tuple3<String, String, Long>> bStream = env.socketTextStream("localhost", 9101, "\n")
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner(new SerializableTimestampAssigner<String>() {
                                    @Override
                                    public long extractTimestamp(String element, long recordTimestamp) {
                                        String[] params = element.split(",");
                                        return Long.parseLong(params[1]);
                                    }
                                })
                )
                // 添加 ProcessFunction 用于输出 B 流的 Watermark
                .process(new ProcessFunction<String, Tuple3<String, String, Long>>() {
                    @Override
                    public void processElement(String element, Context ctx, Collector<Tuple3<String, String, Long>> out) throws Exception {
                        String[] params = element.split(",");
                        String word = params[0];
                        Long timestamp = Long.parseLong(params[1]);
                        Long watermark = ctx.timerService().currentWatermark();
                        LOG.info("BStream word: {}, timestamp: {}, watermark: {}", word, timestamp, watermark);
                        out.collect(Tuple3.of("BStream", word, timestamp));
                    }
                });

        // 合并流
        DataStream<Tuple3<String, String, Long>> unionStream = aStream.union(bStream);
        SingleOutputStreamOperator<Tuple3<String, String, Long>> processStream = unionStream.keyBy(new KeySelector<Tuple3<String,String,Long>, String>() {
            @Override
            public String getKey(Tuple3<String, String, Long> element) throws Exception {
                return element.f1;
            }
        }).process(new KeyedProcessFunction<String, Tuple3<String, String, Long>, Tuple3<String, String, Long>>() {
            @Override
            public void processElement(Tuple3<String, String, Long> element, Context ctx, Collector<Tuple3<String, String, Long>> out) throws Exception {
                Long watermark = ctx.timerService().currentWatermark();
                LOG.info("UnionStream word: {}, timestamp: {}, watermark: {}", element.f1, element.f2, watermark);
                out.collect(element);
            }
        });
        // 输出
        processStream.print();

        env.execute("UnionStreamExample");
    }
}