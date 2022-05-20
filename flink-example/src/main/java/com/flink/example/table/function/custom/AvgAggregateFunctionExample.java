package com.flink.example.table.function.custom;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * 功能：AvgAggregateFunction 自定义聚合函数调用示例
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/5/20 上午9:25
 */
public class AvgAggregateFunctionExample {
    public static void main(String[] args) throws Exception {
        // 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        DataStream<Row> sourceStream = env.fromElements(
                Row.of("李雷", "语文", 58),
                Row.of("韩梅梅", "英语", 90),
                Row.of("李雷", "英语", 95),
                Row.of("韩梅梅", "语文", 85)
        );

        // 注册虚拟表
        tEnv.createTemporaryView("stu_score", sourceStream, $("name"), $("course"), $("score"));

        // 1. Table API 内联方式
        run1(tEnv);

        // 2. Table API 注册临时系统函数
        //run2(tEnv);

        // 3. SQL 注册临时系统函数
        //run3(tEnv);
    }

    // Table API 内联方式
    private static void run1(StreamTableEnvironment tEnv) {
        tEnv.from("stu_score")
                .groupBy($("course"))
                .select($("course"), call(AvgAggregateFunction.class, $("score")).as("avg_score"))
                .execute()
                .print();
    }

    // Table API 注册临时系统函数
    private static void run2(StreamTableEnvironment tEnv) {
        tEnv.createTemporarySystemFunction("custom_avg", AvgAggregateFunction.class);
        tEnv.from("stu_score")
                .groupBy($("course"))
                .select($("course"), call("custom_avg", $("score")).as("avg_score"))
                .execute()
                .print();
    }

    // SQL 注册临时系统函数
    private static void run3(StreamTableEnvironment tEnv) {
        tEnv.createTemporarySystemFunction("custom_avg", new AvgAggregateFunction());
        tEnv.sqlQuery("SELECT course, custom_avg(score) AS avg_score FROM stu_score GROUP BY course")
                .execute()
                .print();
    }
}
