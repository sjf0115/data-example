package com.flink.example.stream.function.process;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/**
 * 功能：使用 ProcessFunction 示例
 *      包含错误信息的元素输出到侧输出流中
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2022/9/13 上午9:33
 */
public class ProcessFunctionExample {
    private static final Logger LOG = LoggerFactory.getLogger(ProcessFunctionExample.class);
    private static OutputTag<Tuple2<String, Long>> errorOutputTag = new OutputTag<Tuple2<String, Long>>("ERROR"){};

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 输入源 <message, timestamp>
        DataStream<String> source = env.socketTextStream("localhost", 9100, "\n");
        DataStream<Tuple2<String, Long>> stream = source.map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String str) throws Exception {
                String[] params = str.split(",");
                String message = params[0];
                Long timestamp = Long.parseLong(params[1]);
                LOG.info("输入元素 message: {}, timestamp: {}",  message, timestamp);
                return Tuple2.of(message, timestamp);
            }
        });

        // 逻辑处理
        SingleOutputStreamOperator<Tuple2<String, Long>> processStream = stream
                // 如果使用事件时间必须设置Timestamp提取和Watermark生成 否则下游 ctx.timestamp() 为null
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                                    @Override
                                    public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
                                        return element.f1;
                                    }
                                })
                )
                // 数据处理 包含错误信息的元素输出到侧输出流中
                .process(new ProcessFunction<Tuple2<String, Long>, Tuple2<String, Long>>() {
                    @Override
                    public void processElement(Tuple2<String, Long> element, Context ctx, Collector<Tuple2<String, Long>> out) throws Exception {
                        String message = element.f0;
                        // Watermark
                        Long watermark = ctx.timerService().currentWatermark();
                        // 元素事件时间戳
                        Long timestamp = ctx.timestamp();
                        // 包含错误信息
                        if (message.contains("error")) {
                            // 侧输出
                            ctx.output(errorOutputTag, element);
                        } else {
                            // 主输出
                            out.collect(element);
                        }
                        LOG.info("元素处理 key: {}, timestamp: {}, watermark: {}", message, timestamp, watermark);
                    }
                });

        // 输出
        processStream.print("MAIN");
        processStream.getSideOutput(errorOutputTag).print("ERROR");
        env.execute("ProcessFunctionExample");
    }
}
// the main method caused an error: Unable to instantiate java compiler,1663077600000
// frequency is not found in word,1663077605000
// Setting timers is only supported on a keyed streams,1663077610000