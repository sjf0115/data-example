package com.flink.example.stream.window.assigner;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 功能：处理时间会话窗口示例
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2022/8/31 上午8:11
 */
public class ProcessingTimeSessionWindowExample {
    private static final Logger LOG = LoggerFactory.getLogger(ProcessingTimeWindowExample.class);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置 Checkpoint
        env.enableCheckpointing(1000L);
        // 设置处理时间特性
        env.getConfig().setAutoWatermarkInterval(0);

        // Socket Source
        DataStream<String> source = env.socketTextStream("localhost", 9100, "\n");

        // 会话窗口
        SingleOutputStreamOperator<String> sessionStream = source
                .keyBy(new KeySelector<String, String>() {
                    @Override
                    public String getKey(String value) throws Exception {
                        return value;
                    }
                })
                // 会话窗口
                .window(ProcessingTimeSessionWindows.withGap(Time.seconds(30)))
                // 求和
                .sum(1);
        // 输出
        sessionStream.print();
        env.execute("ProcessingTimeSessionWindowExample");
    }
}
