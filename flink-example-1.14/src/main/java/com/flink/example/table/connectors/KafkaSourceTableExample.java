package com.flink.example.table.connectors;

import com.google.common.collect.Lists;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

/**
 * 功能：Kafka Source Table
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/4/9 下午7:27
 */
public class KafkaSourceTableExample {
    public static void main(String[] args) throws Exception {

        // 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // Kafka Table Schema
        Schema schema = Schema.newBuilder()
                .column("word", DataTypes.STRING())
                .column("frequency", DataTypes.BIGINT())
                .build();

        // Kafka Table TableDescriptor
        TableDescriptor descriptor = TableDescriptor.forConnector("kafka")
                .comment("kafka source table")
                .schema(schema)
                .option(KafkaConnectorOptions.TOPIC, Lists.newArrayList("word"))
                .option(KafkaConnectorOptions.PROPS_BOOTSTRAP_SERVERS, "localhost:9092")
                .option(KafkaConnectorOptions.PROPS_GROUP_ID, "kafka-table-descriptor")
                .option("scan.startup.mode", "earliest-offset")
                .format("json")
                .build();

        // 注册表
        tEnv.createTemporaryTable("kafka_source_table", descriptor);

        // 转换为 Table
        Table table = tEnv.from("kafka_source_table");

        // 执行查询
        Table resultTable = table
                .groupBy($("word"))
                .select($("word"), $("frequency").sum())
                .as("word", "frequency");

        // Table 转 DataStream
        DataStream<Row> result = tEnv.toChangelogStream(resultTable);
        result.print();

        // 执行
        env.execute();
    }
}
