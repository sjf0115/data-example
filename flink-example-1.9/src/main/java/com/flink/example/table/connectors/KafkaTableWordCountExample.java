package com.flink.example.table.connectors;

import com.common.example.bean.WordCount;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;

/**
 * 功能：Table 版 WordCount Kafka 输入源
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/4/9 下午6:19
 */
public class KafkaTableWordCountExample {
    public static void main(String[] args) throws Exception {
        // 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 注册表 source_table
        tEnv.connect(new Kafka()
                    .version("universal")
                    .topic("word")
                    .property("group.id", "kafka-table-connect")
                    .property("zookeeper.connect", "localhost:2181")
                    .property("bootstrap.servers", "localhost:9092")
                    .startFromEarliest()
                )
                .withFormat(new Json().deriveSchema())
                .withSchema(
                        new Schema()
                                .field("word", Types.STRING)
                                .field("frequency", Types.LONG)
                )
                .inAppendMode()
                .registerTableSource("source_table");

        // 扫描注册表转换为 Table
        Table wordTable = tEnv.scan("source_table");

        // 执行查询
        Table resultTable = wordTable
                .groupBy("word")
                .select("word, frequency.sum as frequency");

        // Table 转 DataStream
        DataStream<Tuple2<Boolean, WordCount>> result = tEnv.toRetractStream(resultTable, WordCount.class);
        result.print();

        // 执行
        env.execute();
    }
}
