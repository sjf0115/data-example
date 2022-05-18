package com.flink.example.table.function.custom;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * 功能：SplitTableFunction 示例
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/5/18 下午11:23
 */
public class SplitTableFunctionExample {
    public static void main(String[] args) throws Exception {
        // 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        DataStream<Row> sourceStream = env.fromElements(
                Row.of("a b c"),
                Row.of("abc ab")
        );

        // 注册虚拟表
        tEnv.createTemporaryView("user_behavior", sourceStream, $("str"));

        // 1. Table API
        run1(tEnv);

        // 2. SQL
        //run2(tEnv);
    }

    // Table API
    private static void run1(StreamTableEnvironment tEnv) {
        tEnv.from("user_behavior")
                .joinLateral(call(SplitTableFunction.class, $("str")).as("word", "length"))
                .select($("str"), $("word"), $("length"))
                .execute()
                .print();
    }

    // SQL
    private static void run2(StreamTableEnvironment tEnv) {
        tEnv.createTemporarySystemFunction("SplitFunction", SplitTableFunction.class);
        tEnv.sqlQuery(
                "SELECT str, word, length " +
                        "FROM user_behavior, LATERAL TABLE(SplitFunction(str))")
                .execute()
                .print();
    }
}
