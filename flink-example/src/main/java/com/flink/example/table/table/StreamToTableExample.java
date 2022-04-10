package com.flink.example.table.table;

import com.common.example.bean.User;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Instant;

import static org.apache.flink.table.api.Expressions.$;

/**
 * 功能：流转换为表
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/4/10 上午11:58
 */
public class StreamToTableExample {
    public static void main(String[] args) throws Exception {
        // Stream 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // Table 执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 读取数据创建 DataStream
        DataStream<User> dataStream =
                env.fromElements(
                        new User("Alice", 4, Instant.ofEpochMilli(1000)),
                        new User("Bob", 6, Instant.ofEpochMilli(1001)),
                        new User("Alice", 10, Instant.ofEpochMilli(1002))
                );

        // DataStream 转 Table

        // 1. 自动派生所有物理列
        Table table = tableEnv.fromDataStream(dataStream);
        table.printSchema();

        // 2. 自动派生所有物理列 但使用表达式方式指定提取的字段以及位置
        Table table2 = tableEnv.fromDataStream(dataStream, $("name"), $("score"), $("eventTime"));
        table2.printSchema();

        // 3. 自动派生所有物理列 但使用字符串方式指定提取的字段以及位置
        Table table3 = tableEnv.fromDataStream(dataStream, "score, name, eventTime");
        table3.printSchema();

        // 4. 自动派生所有物理列 但添加计算列创建 proc_time 属性列
        Schema schema4 = Schema.newBuilder()
                .columnByExpression("proc_time", "PROCTIME()")
                .build();
        Table table4 = tableEnv.fromDataStream(dataStream, schema4);
        table4.printSchema();

        // 5. 自动派生所有物理列 但添加计算列创建 rowtime 属性列并自定义 Watermark 策略
        Schema schema5 = Schema.newBuilder()
                .columnByExpression("rowtime", "CAST(eventTime AS TIMESTAMP_LTZ(3))")
                .watermark("rowtime", "rowtime - INTERVAL '10' SECOND")
                .build();
        Table table5 = tableEnv.fromDataStream(dataStream, schema5);
        table5.printSchema();

        // 6. 自动派生所有物理列 但访问流记录的时间戳来创建时间属性列并依赖于 DataStream API 中生成的 Watermark
        Schema schema6 = Schema.newBuilder()
                .columnByMetadata("rowtime", "TIMESTAMP_LTZ(3)")
                .watermark("rowtime", "SOURCE_WATERMARK()")
                .build();
        Table table6 = tableEnv.fromDataStream(dataStream, schema6);
        table6.printSchema();

        // 7. 手动定义物理列 但时间戳的精度从9降到3并将其放在第一个位置
        Schema schema7 = Schema.newBuilder()
                .column("eventTime", "TIMESTAMP_LTZ(3)")
                .column("name", "STRING")
                .column("score", "INT")
                .watermark("eventTime", "SOURCE_WATERMARK()")
                .build();
        Table table7 = tableEnv.fromDataStream(dataStream, schema7);
        table7.printSchema();
    }
}
