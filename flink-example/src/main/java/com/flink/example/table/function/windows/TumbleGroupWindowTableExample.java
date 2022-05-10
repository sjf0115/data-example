package com.flink.example.table.function.windows;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

/**
 * 功能：Group Window 滚动窗口 Table API 示例
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/5/5 下午5:34
 */
public class TumbleGroupWindowTableExample {
    public static void main(String[] args) {
        // 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        DataStream<Row> inputStream = env.fromElements(
                Row.of("1000", "2017-11-27 13:01:21", 1511758881000L),
                Row.of("1000", "2017-11-27 17:00:14", 1511773214000L),
                Row.of("1000", "2017-11-27 23:39:27", 1511797167000L),
                Row.of("1001", "2017-11-27 13:00:31", 1511758831000L),
                Row.of("1001", "2017-11-27 23:45:59", 1511797559000L)
        );
        Table inputTable = tEnv.fromDataStream(inputStream, $("uid"), $(""));

        Table outputTable = inputTable
                //.window(Tumble.over("10.minutes").on("rowtime").as("w"))
                .window(Tumble.over(lit(1).minutes()).on($("rowtime")).as("w"))
                .groupBy($("w"), $("uid")) // 根据时间窗口 w 和用户 uid 分组
                .select($("uid"), $("w").start(), $("w").end(), $("w").rowtime(), $("b").count());

    }
}
