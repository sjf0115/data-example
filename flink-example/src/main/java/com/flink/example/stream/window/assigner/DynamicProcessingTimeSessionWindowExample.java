package com.flink.example.stream.window.assigner;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.DynamicProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SessionWindowTimeGapExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 功能：动态会话窗口示例
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2022/8/31 上午8:25
 */
public class DynamicProcessingTimeSessionWindowExample {
    private static final Logger LOG = LoggerFactory.getLogger(ProcessingTimeWindowExample.class);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置 Checkpoint
        env.enableCheckpointing(1000L);
        // 设置处理时间特性
        env.getConfig().setAutoWatermarkInterval(0);

        // Socket Source
        DataStream<String> source = env.socketTextStream("localhost", 9100, "\n");

        // 动态会话窗口
        SingleOutputStreamOperator<Tuple2<String, Long>> dynamicStream = source
                .map(new MapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(String value) throws Exception {
                        String[] params = value.split(",");
                        String uid = params[0];
                        Long gap = Long.parseLong(params[1]);
                        return Tuple2.of(uid, gap);
                    }
                })
                .keyBy(new KeySelector<Tuple2<String, Long>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Long> tuple2) throws Exception {
                        return tuple2.f0;
                    }
                })
                // 会话窗口
                .window(DynamicProcessingTimeSessionWindows.withDynamicGap(new SessionWindowTimeGapExtractor<Tuple2<String, Long>>() {
                    @Override
                    public long extract(Tuple2<String, Long> element) {
                        // 动态提取时间间隔 Gap
                        return element.f1;
                    }
                }))
                // 求和
                .sum(1);
        // 输出
        dynamicStream.print();
        env.execute("DynamicProcessingTimeSessionWindowExample");
    }
}
