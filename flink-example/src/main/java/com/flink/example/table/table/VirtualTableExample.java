package com.flink.example.table.table;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

/**
 * 功能：虚拟表创建示例
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/4/10 上午10:57
 */
public class VirtualTableExample {
    public static void main(String[] args) throws Exception {
        // 创建流和表执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 创建 DataStream
        DataStream<String> dataStream = env.fromElements("Alice", "Bob", "John");

        // 1. 将 Table 注册为虚拟表
        Table inputTable = tableEnv.fromDataStream(dataStream, $("f0"));
        tableEnv.createTemporaryView("inputTableView", inputTable);
        Table upperTable = tableEnv.sqlQuery("SELECT UPPER(f0) FROM inputTableView");
        DataStream<Row> upperStream = tableEnv.toDataStream(upperTable);
        upperStream.print("U");

        // 2. 将 DataStream 注册为虚拟表
        // 2.1 自动派生所有列
        tableEnv.createTemporaryView("inputStreamView", dataStream);
        // 2.2 自动派生所有列 但使用表达式方式指定提取的字段以及位置
        //tableEnv.createTemporaryView("inputStreamView2", dataStream, $("f0"));
        // 2.3 手动定义列
        /*Schema schema = Schema.newBuilder()
                .column("f0", DataTypes.STRING())
                .build();
        tableEnv.createTemporaryView("inputStreamView3", dataStream, schema);*/
        Table lowerTable = tableEnv.sqlQuery("SELECT LOWER(f0) FROM inputStreamView");
        DataStream<Row> lowerStream = tableEnv.toDataStream(lowerTable);
        lowerStream.print("L");

        env.execute();
    }
}
//U:4> +I[JOHN]
//L:4> +I[bob]
//L:3> +I[alice]
//U:3> +I[BOB]
//L:1> +I[john]
//U:2> +I[ALICE]