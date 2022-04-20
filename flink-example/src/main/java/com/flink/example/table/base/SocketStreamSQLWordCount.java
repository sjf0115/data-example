package com.flink.example.table.base;

import com.common.example.bean.WordCount;
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
 * 功能：SQL 版 WordCount: Socket Source & Streaming
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/4/1 下午10:59
 */
public class SocketStreamSQLWordCount {
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
        DataStream<WordCount> wordsStream = lines.flatMap(new FlatMapFunction<String, WordCount>() {
            @Override
            public void flatMap(String value, Collector<WordCount> out) throws Exception {
                for (String word : value.split("\\s")) {
                    out.collect(new WordCount(word, 1L));
                }
            }
        });

        // 注册虚拟表
        tabEnv.createTemporaryView("tb_words", wordsStream, $("word"), $("frequency"));

        // SQL 语句
        String sql = "SELECT word, SUM(frequency) AS frequency\n" +
                "FROM tb_words\n" +
                "GROUP BY word";

        // 执行 SQL
        Table resultTable = tabEnv.sqlQuery(sql);

        // Table 转 DataStream
        DataStream<Tuple2<Boolean, WordCount>> resultStream = tabEnv.toRetractStream(resultTable, WordCount.class);

        // 输出结果
        resultStream.print();

        // 执行
        env.execute();
    }
}
