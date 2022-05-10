package com.flink.example.table.time;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

/**
 * 功能：时间属性 DataStream 转 Table 时定义
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/5/5 下午6:03
 */
public class TimeAttributeTableExample {
    public static void main(String[] args) {
        // 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 1. 示例1 处理时间属性
        runProcessTimeExample(env, tEnv);
        // 2. 示例2 事件时间属性
        runEventTimeExample(env, tEnv);
    }

    // 示例1 处理时间属性
    private static void runProcessTimeExample(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {
        DataStream<Row> inputStream = env.fromElements(
                Row.of("1000", "2017-11-27 13:01:21", 1511758881000L),
                Row.of("1000", "2017-11-27 17:00:14", 1511773214000L),
                Row.of("1000", "2017-11-27 23:39:27", 1511797167000L),
                Row.of("1001", "2017-11-27 13:00:31", 1511758831000L),
                Row.of("1001", "2017-11-27 23:45:59", 1511797559000L)
        );
        Table table = tEnv.fromDataStream(inputStream, $("uid"), $("time"), $("ts"), $("process_time").proctime());
        table.printSchema();
    }

    // 示例2 事件时间属性
    private static void runEventTimeExample(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {
        DataStream<Row> inputStream = env.fromElements(
                Row.of("1000", "2017-11-27 13:01:21", 1511758881000L),
                Row.of("1000", "2017-11-27 17:00:14", 1511773214000L),
                Row.of("1000", "2017-11-27 23:39:27", 1511797167000L),
                Row.of("1001", "2017-11-27 13:00:31", 1511758831000L),
                Row.of("1001", "2017-11-27 23:45:59", 1511797559000L)
        );
        Table table = tEnv.fromDataStream(inputStream, $("uid"), $("time"), $("ts"), $("process_time").rowtime());
        table.printSchema();
    }
}
