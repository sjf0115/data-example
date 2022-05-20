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

        // 1. Table API JoinLateral
        run1(tEnv);

        // 2. Table API LeftOuterJoinLateral
        //run2(tEnv);

        // 3. SQL LATERAL TABLE Join 语法
        //run3(tEnv);

        // 4. SQL LATERAL TABLE Join 语法 重命名字段
        //run4(tEnv);

        // 5. SQL LATERAL TABLE Left Join 语法
        //run5(tEnv);

        // 6. SQL LATERAL TABLE Left Join 语法 重命名字段
        //run6(tEnv);
    }

    // Table API joinLateral
    private static void run1(StreamTableEnvironment tEnv) {
        tEnv.from("stu_score")
                .groupBy($("course"))
                .select($("course"), call(AvgAggregateFunction.class, $("score")))
                .execute()
                .print();
    }


}
