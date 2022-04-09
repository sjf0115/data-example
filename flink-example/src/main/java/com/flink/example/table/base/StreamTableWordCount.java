package com.flink.example.table.base;

import com.flink.example.bean.WordCount;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import static org.apache.flink.table.api.Expressions.$;

/**
 * 功能：Table API WordCount
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/3/31 下午9:22
 */
public class StreamTableWordCount {
    public static void main(String[] args) throws Exception {
        // Table 运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tabEnv = StreamTableEnvironment.create(env, settings);

        // Socket 数据源
        DataStream<String> lines = env.socketTextStream("localhost", 9100, "\n");
        // 单词拆分
        DataStream<WordCount> words = lines.flatMap(new FlatMapFunction<String, WordCount>() {
            @Override
            public void flatMap(String value, Collector<WordCount> out) throws Exception {
                for (String word : value.split("\\s")) {
                    out.collect(new WordCount(word, 1L));
                }
            }
        });

        // DataStream 转 Table
        Table table = tabEnv.fromDataStream(words, $("word"), $("frequency"));
        Table resultTable = table.groupBy($("word"))
                .select($("word"), $("frequency").sum())
                .as("word", "frequency");

        // Table 转换为 DataStream
        DataStream<Tuple2<Boolean, WordCount>> resultStream = tabEnv.toRetractStream(resultTable, WordCount.class);

        // 输出结果
        resultStream.print();

        // 执行
        env.execute();
    }
}
