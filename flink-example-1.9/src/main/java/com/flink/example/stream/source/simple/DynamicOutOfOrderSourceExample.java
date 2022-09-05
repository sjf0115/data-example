package com.flink.example.stream.source.simple;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 功能：DynamicOutOfOrderSource 示例
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2022/9/4 下午10:29
 */
public class DynamicOutOfOrderSourceExample {
    private static final Logger LOG = LoggerFactory.getLogger(DynamicOutOfOrderSourceExample.class);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置 Checkpoint
        env.enableCheckpointing(1000L);
        // 设置事件时间特性
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 输入源 每1s输出一次
        DataStream<Tuple3<String, Integer, Long>> source = env.addSource(new DynamicOutOfOrderSource());
        // 输出
        source.print();
        env.execute("DynamicOutOfOrderSourceExample");
    }
}
