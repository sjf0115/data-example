package com.flink.example.table.base;

import com.common.example.bean.WordCount;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * 功能：
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/4/10 上午11:08
 */
public class StreamSQLSimpleWordCount {
    public static void main(String[] args) throws Exception {
        // 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 默认 BlinkPlanner
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 读取数据创建 DataStream
        DataStream<WordCount> input = env.fromElements(
                new WordCount("Hello", 1L),
                new WordCount("Ciao", 1L),
                new WordCount("Hello", 1L));

        // 注册 Table
        tEnv.createTemporaryView("WordCount", input, $("word"), $("frequency"));

        // 执行 SQL 查询
        Table table = tEnv.sqlQuery(
                "SELECT word, SUM(frequency) as frequency FROM WordCount GROUP BY word");

        // Table 转换为 DataSet
        DataStream<Tuple2<Boolean, WordCount>> result = tEnv.toRetractStream(table, WordCount.class);
        result.print();

        // 执行
        env.execute();
    }
}
