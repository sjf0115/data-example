package com.flink.example.stream.function.process;

import com.common.example.utils.DateUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.time.Duration;

/**
 * 功能：使用 ProcessFunction 示例
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2022/9/13 上午9:33
 */
public class ProcessFunctionSimpleExample {
    private static final Logger LOG = LoggerFactory.getLogger(ProcessFunctionSimpleExample.class);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 输入源
        DataStream<String> source = env.socketTextStream("localhost", 9100, "\n");

        DataStream<Tuple2<String, String>> stream = source.map(new MapFunction<String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(String value) throws Exception {
                String[] params = value.split(",");
                String key = params[0];
                String eventTime = params[1];
                return new Tuple2<>(key, eventTime);
            }
        });

        // 如果使用事件时间必须设置Timestamp提取和Watermark生成 否则下游 ctx.timestamp() 为null
        DataStream<Long> result = stream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<Tuple2<String, String>>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, String>>() {
                            @Override
                            public long extractTimestamp(Tuple2<String, String> element, long recordTimestamp) {
                                Long timeStamp = 0L;
                                try {
                                    timeStamp = DateUtil.date2TimeStamp(element.f1, "yyyy-MM-dd HH:mm:ss");
                                } catch (ParseException e) {
                                    e.printStackTrace();
                                }
                                return timeStamp;
                            }
                        })
        ).process(new ProcessFunction<Tuple2<String, String>, Long>() {
            @Override
            public void processElement(Tuple2<String, String> value, Context ctx, Collector<Long> out) throws Exception {
                String key = value.f0;
                String eventTime = value.f1;
                Long timestamp = ctx.timestamp();
                LOG.info("[ProcessElement] Key: {}, EventTime: {}, Timestamp: {}", key, eventTime, timestamp);
            }
        });

        result.print();
        env.execute("ProcessFunctionSimpleExample");
    }
}
