package com.flink.example.table.function.custom;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * 功能：自定义函数调用示例
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/5/17 下午10:50
 */
public class CustomFunctionCallExample {

    public static void main(String[] args) throws Exception {
        // 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        DataStream<Row> sourceStream = env.fromElements(
                Row.of(1, 2),
                Row.of(2, 3),
                Row.of(3, 4),
                Row.of(4, 5)
        );

        // 注册虚拟表
        tEnv.createTemporaryView("user_behavior", sourceStream, $("a"), $("b"));

        // 1. Table API 用 call 函数内联方式调用函数类 不需要注册
        run1(tEnv);

        // 2. Table API 用 call 函数内联方式调用函数实例 不需要注册
        //run2(tEnv);

        // 3. Table API 使用函数类注册临时系统函数 通过 call 函数调用
        //run3(tEnv);

        // 4. Table API 使用函数实例注册临时系统函数 通过 call 函数调用
        //run4(tEnv);

        // 5. SQL 注册临时系统函数 通过注册的名字调用
        //run5(tEnv);
    }

    // Table API 用 call 函数内联方式调用函数类 不需要注册
    private static void run1(StreamTableEnvironment tEnv) {
        tEnv.from("user_behavior")
                .select(call(AddScalarFunction.class, $("a"), $("b")).as("sum1"))
                .execute()
                .print();
    }

    // Table API 用 call 函数内联方式调用函数实例 不需要注册
    private static void run2(StreamTableEnvironment tEnv) {
        tEnv.from("user_behavior")
                .select(call(new AddScalarFunction(), $("a"), $("b")).as("sum2"))
                .execute()
                .print();
    }

    // Table API 使用函数类注册临时系统函数 通过 call 函数调用
    private static void run3(StreamTableEnvironment tEnv) {
        // 注册函数
        tEnv.createTemporarySystemFunction("AddScalarFunction", AddScalarFunction.class);
        // 使用 call 函数调用已注册的函数
        tEnv.from("user_behavior")
                .select(call("AddScalarFunction", $("a"), $("b")).as("sum3"))
                .execute()
                .print();
    }

    // Table API 使用函数实例注册临时系统函数 通过 call 函数调用
    private static void run4(StreamTableEnvironment tEnv) {
        // 注册函数
        tEnv.createTemporarySystemFunction("AddScalarFunction", new AddScalarFunction());
        // 使用 call 函数调用已注册的函数
        tEnv.from("user_behavior")
                .select(call("AddScalarFunction", $("a"), $("b")).as("sum4"))
                .execute()
                .print();
    }

    // SQL 注册临时系统函数 通过注册的名字调用
    private static void run5(StreamTableEnvironment tEnv) {
        tEnv.createTemporarySystemFunction("AddScalarSQLFunction", AddScalarFunction.class);
        tEnv.sqlQuery("SELECT AddScalarSQLFunction(a, b) AS sum5 FROM user_behavior")
                .execute()
                .print();
    }
}
