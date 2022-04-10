package com.flink.example.table.table;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

/**
 * 功能：Table API 输出表简单示例
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/4/10 下午9:16
 */
public class TableOutputExample {
    public static void main(String[] args) throws Exception {
        // 创建流和表执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 创建 DataStream
        DataStream<Row> dataStream = env.fromElements(
                Row.of("Alice", 12),
                Row.of("Bob", 10),
                Row.of("Alice", 100),
                Row.of("Lucy", 50)
        );
        // 将 DataStream 转换为 Table
        Table inputTable = tableEnv.fromDataStream(dataStream).as("name", "score");

        // 聚合计算
        Table outputTable = inputTable
                .filter($("name").isNotEqual("Lucy"))
                .groupBy($("name"))
                .select($("name"), $("score").sum().as("score_sum"));

        // 创建 Print Connector 表
        tableEnv.createTemporaryTable(
                "print_sink_table",
                TableDescriptor.forConnector("print")
                .schema(Schema.newBuilder()
                        .column("name", DataTypes.STRING())
                        .column("score_sum", DataTypes.BIGINT())
                        .build()
                )
                .build()
        );

        // 输出
        outputTable.executeInsert("print_sink_table");
    }
}
