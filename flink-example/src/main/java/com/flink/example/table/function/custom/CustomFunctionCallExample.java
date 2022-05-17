package com.flink.example.table.function.custom;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

/**
 * 功能：自定义函数调用示例
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/5/17 下午10:50
 */
public class CustomFunctionCallExample {

    public static void main(String[] args) {
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

        //tEnv.from("user_behavior").select(call(AddScalarFunction.class, $("sum"), 5, 12));
    }
}
