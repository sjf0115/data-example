package com.flink.example.base;

import com.common.example.bean.WordCount;
import com.common.example.utils.FlinkOptions;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import static org.apache.flink.table.api.Expressions.$;

/**
 * 功能：本地运行 Web UI 示例
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/5/12 下午10:22
 */
public class LocalWebUIExample {
    public static void main(String[] args) throws Exception {

        // 本地运行
        Configuration configuration = new Configuration();
        configuration.setString(RestOptions.BIND_PORT, FlinkOptions.WEB_UI_BIND_PORT);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // Table 运行环境
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        EnvironmentSettings settings = EnvironmentSettings
//                .newInstance()
//                .useBlinkPlanner()
//                .inStreamingMode()
//                .build();
//        StreamTableEnvironment tabEnv = StreamTableEnvironment.create(env, settings);

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
        Table table = tEnv.fromDataStream(words, $("word"), $("frequency"));
        Table resultTable = table.groupBy($("word"))
                .select($("word"), $("frequency").sum())
                .as("word", "frequency");

        // Table 转换为 DataStream
        DataStream<Tuple2<Boolean, WordCount>> resultStream = tEnv.toRetractStream(resultTable, WordCount.class);

        // 输出结果
        resultStream.print();

        // 执行
        env.execute();
    }
}
