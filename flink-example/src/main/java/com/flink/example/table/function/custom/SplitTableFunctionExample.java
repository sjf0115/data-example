package com.flink.example.table.function.custom;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * 功能：SplitTableFunction 表函数调用示例
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
                Row.of("lucy", "apple,banana"),
                Row.of("lily", "banana,peach,watermelon"),
                Row.of("tom", null)
        );

        // 注册虚拟表
        tEnv.createTemporaryView("like_fruits", sourceStream, $("name"), $("fruits"));

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
        tEnv.from("like_fruits")
                .joinLateral(call(SplitTableFunction.class, $("fruits")).as("fruit"))
                .select($("name"), $("fruit"))
                .execute()
                .print();
    }

    // Table API leftOuterJoinLateral
    private static void run2(StreamTableEnvironment tEnv) {
        tEnv.from("like_fruits")
                .leftOuterJoinLateral(call(new SplitTableFunction(","), $("fruits")).as("fruit"))
                .select($("name"), $("fruit"))
                .execute()
                .print();
    }

    // SQL LATERAL TABLE Join 语法
    private static void run3(StreamTableEnvironment tEnv) {
        tEnv.createTemporarySystemFunction("SplitFunction", new SplitTableFunction(","));
        tEnv.sqlQuery("SELECT name, word \n" +
                "FROM like_fruits,\n" +
                "LATERAL TABLE(SplitFunction(fruits))")
                .execute()
                .print();
    }

    // SQL LATERAL TABLE Join 语法 重命名字段
    private static void run4(StreamTableEnvironment tEnv) {
        tEnv.createTemporarySystemFunction("SplitFunction", new SplitTableFunction(","));
        tEnv.sqlQuery("SELECT a.name, b.fruit \n" +
                "FROM like_fruits AS a,\n" +
                "LATERAL TABLE(SplitFunction(fruits)) AS b(fruit)")
                .execute()
                .print();
    }

    // SQL LATERAL TABLE Join 语法
    private static void run5(StreamTableEnvironment tEnv) {
        tEnv.createTemporarySystemFunction("SplitFunction", new SplitTableFunction(","));
        tEnv.sqlQuery("SELECT name, word \n" +
                "FROM like_fruits\n" +
                "LEFT JOIN LATERAL TABLE(SplitFunction(fruits)) ON TRUE")
                .execute().print();
    }

    // SQL LATERAL TABLE Join 语法 重命名字段
    private static void run6(StreamTableEnvironment tEnv) {
        tEnv.createTemporarySystemFunction("SplitFunction", new SplitTableFunction(","));
        tEnv.sqlQuery("SELECT a.name, b.fruit \n" +
                "FROM like_fruits AS a\n" +
                "LEFT JOIN LATERAL TABLE(SplitFunction(fruits)) AS b(fruit) ON TRUE")
                .execute()
                .print();
    }
}
//+----+--------------------------------+--------------------------------+
//| op |                           name |                          fruit |
//+----+--------------------------------+--------------------------------+
//| +I |                           lucy |                          apple |
//| +I |                           lucy |                         banana |
//| +I |                           lily |                         banana |
//| +I |                           lily |                          peach |
//| +I |                           lily |                     watermelon |
//+----+--------------------------------+--------------------------------+


//+----+--------------------------------+--------------------------------+
//| op |                           name |                          fruit |
//+----+--------------------------------+--------------------------------+
//| +I |                           lucy |                          apple |
//| +I |                           lucy |                         banana |
//| +I |                           lily |                         banana |
//| +I |                           lily |                          peach |
//| +I |                           lily |                     watermelon |
//| +I |                            tom |                         (NULL) |
//+----+--------------------------------+--------------------------------+


//+----+--------------------------------+--------------------------------+
//| op |                           name |                           word |
//+----+--------------------------------+--------------------------------+
//| +I |                           lucy |                          apple |
//| +I |                           lucy |                         banana |
//| +I |                           lily |                         banana |
//| +I |                           lily |                          peach |
//| +I |                           lily |                     watermelon |
//+----+--------------------------------+--------------------------------+


//+----+--------------------------------+--------------------------------+
//| op |                           name |                          fruit |
//+----+--------------------------------+--------------------------------+
//| +I |                           lucy |                          apple |
//| +I |                           lucy |                         banana |
//| +I |                           lily |                         banana |
//| +I |                           lily |                          peach |
//| +I |                           lily |                     watermelon |
//+----+--------------------------------+--------------------------------+


//+----+--------------------------------+--------------------------------+
//| op |                           name |                           word |
//+----+--------------------------------+--------------------------------+
//| +I |                           lucy |                          apple |
//| +I |                           lucy |                         banana |
//| +I |                           lily |                         banana |
//| +I |                           lily |                          peach |
//| +I |                           lily |                     watermelon |
//| +I |                            tom |                         (NULL) |
//+----+--------------------------------+--------------------------------+


//+----+--------------------------------+--------------------------------+
//| op |                           name |                          fruit |
//+----+--------------------------------+--------------------------------+
//| +I |                           lucy |                          apple |
//| +I |                           lucy |                         banana |
//| +I |                           lily |                         banana |
//| +I |                           lily |                          peach |
//| +I |                           lily |                     watermelon |
//| +I |                            tom |                         (NULL) |
//+----+--------------------------------+--------------------------------+