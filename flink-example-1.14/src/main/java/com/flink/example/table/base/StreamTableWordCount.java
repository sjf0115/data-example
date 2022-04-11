package com.flink.example.table.base;

import org.apache.flink.connector.datagen.table.DataGenConnectorOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * 功能：Table API 版本 WordCount 流处理
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/4/11 下午8:16
 */
public class StreamTableWordCount {
    public static void main(String[] args) throws Exception {
        // 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 创建输入表
        tEnv.createTemporaryTable("source_table",
                TableDescriptor.forConnector("datagen")
                        .schema(Schema.newBuilder()
                                .column("word", DataTypes.STRING())
                                .column("frequency", DataTypes.BIGINT())
                                .build()
                        )
                        .option(DataGenConnectorOptions.ROWS_PER_SECOND, 1L)
                        .option("fields.word.kind", "random")
                        .option("fields.word.length", "1")
                        .option("fields.frequency.min", "1")
                        .option("fields.frequency.max", "9")
                        .build()
        );

        // 聚合查询
        Table resultTable = tEnv.from("source_table")
                .groupBy($("word"))
                .select($("word"), $("frequency").sum().as("frequency"));

        // 创建输出表
        tEnv.createTemporaryTable(
                "sink_table",
                TableDescriptor.forConnector("print")
                        .schema(Schema.newBuilder()
                                .column("word", DataTypes.STRING())
                                .column("frequency", DataTypes.BIGINT())
                                .build()
                        )
                        .build()
        );

        // 输出
        resultTable.executeInsert("sink_table");
    }
}
