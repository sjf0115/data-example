package com.flink.example.table.connectors;

import com.google.common.collect.Lists;
import org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions;
import org.apache.flink.table.api.*;

import static org.apache.flink.table.api.Expressions.$;

/**
 * 功能：Table API 方式创建 Kafka Connector Table 示例
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/4/9 下午7:27
 */
public class KafkaConnectorTableExample {
    public static void main(String[] args) throws Exception {
        // 执行环境
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        // Schema
        Schema schema = Schema.newBuilder()
                .column("word", DataTypes.STRING())
                .column("frequency", DataTypes.BIGINT())
                .build();

        // 创建 Kafka Connector 表
        TableDescriptor kafkaDescriptor = TableDescriptor.forConnector("kafka")
                .comment("kafka source table")
                .schema(schema)
                .option(KafkaConnectorOptions.TOPIC, Lists.newArrayList("word"))
                .option(KafkaConnectorOptions.PROPS_BOOTSTRAP_SERVERS, "localhost:9092")
                .option(KafkaConnectorOptions.PROPS_GROUP_ID, "kafka-table-descriptor")
                .option("scan.startup.mode", "earliest-offset")
                .format("json")
                .build();
        tEnv.createTemporaryTable("kafka_source_table", kafkaDescriptor);

        // 转换为 Table
        Table table = tEnv.from("kafka_source_table");

        // 执行查询
        Table resultTable = table
                .groupBy($("word"))
                .select($("word"), $("frequency").sum())
                .as("word", "frequency");

        // 创建 Print Connector 表
        TableDescriptor printDescriptor = TableDescriptor.forConnector("print")
                .schema(schema)
                .build();
        tEnv.createTemporaryTable("print_sink_table", printDescriptor);

        // 输出
        resultTable.executeInsert("print_sink_table");
    }
}
