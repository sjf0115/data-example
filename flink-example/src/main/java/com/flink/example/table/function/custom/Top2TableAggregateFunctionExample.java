package com.flink.example.table.function.custom;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * 功能：Top2TableAggregateFunction 调用示例
 * 疑问：Blink Planner 异常 Old Planner 可以 ？
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/5/20 下午10:49
 */
public class Top2TableAggregateFunctionExample {
    public static void main(String[] args) throws Exception {
        // 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .useOldPlanner() // Blink Planner 异常 Old Planner 可以
                .inStreamingMode()
                .build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        DataStream<Row> sourceStream = env.fromElements(
                Row.of("李雷", "语文", 78),
                Row.of("韩梅梅", "语文", 50),
                Row.of("李雷", "语文", 99),
                Row.of("韩梅梅", "语文", 80),
                Row.of("李雷", "英语", 90),
                Row.of("韩梅梅", "英语", 40),
                Row.of("李雷", "英语", 98),
                Row.of("韩梅梅", "英语", 88)
        );

        // 注册虚拟表
        tEnv.createTemporaryView("stu_score", sourceStream, $("name"), $("course"), $("score"));

        // 1. Table API 内联 非撤回输出模式 emitValue
        run1(tEnv);

        // 2. Table API 注册临时系统函数 非撤回输出模式 emitValue
        //run2(tEnv);

        // 3. Table API 内联 撤回输出模式 emitUpdateWithRetract
        //run3(tEnv);

        // 4. Table API 注册临时系统函数 撤回输出模式 emitUpdateWithRetract
        //run4(tEnv);
    }

    // Table API 内联 非撤回输出模式 emitValue
    private static void run1(StreamTableEnvironment tEnv) {
        tEnv.from("stu_score")
                .groupBy($("course"))
                .flatAggregate(call(Top2TableAggregateFunction.class, $("score")))
                .select($("course"), $("f0"), $("f1"))
                .execute()
                .print();
    }

    // Table API 注册临时系统函数 非撤回输出模式 emitValue
    private static void run2(StreamTableEnvironment tEnv) {
        tEnv.createTemporarySystemFunction("Top2", new Top2TableAggregateFunction());
        tEnv.from("stu_score")
                .groupBy($("course"))
                .flatAggregate(call("Top2", $("score")).as("score", "rank"))
                .select($("course"), $("score"), $("rank"))
                .execute()
                .print();
    }

    // Table API 内联 撤回输出模式 emitUpdateWithRetract
    private static void run3(StreamTableEnvironment tEnv) {
        tEnv.from("stu_score")
                .groupBy($("course"))
                .flatAggregate(call(Top2RetractTableAggregateFunction.class, $("score")))
                .select($("course"), $("f0"), $("f1"))
                .execute()
                .print();
    }

    // Table API 注册临时系统函数 撤回输出模式 emitUpdateWithRetract
    private static void run4(StreamTableEnvironment tEnv) {
        tEnv.createTemporarySystemFunction("Top2", new Top2RetractTableAggregateFunction());
        tEnv.from("stu_score")
                .groupBy($("course"))
                .flatAggregate(call("Top2", $("score")).as("score", "rank"))
                .select($("course"), $("score"), $("rank"))
                .execute()
                .print();
    }
}
