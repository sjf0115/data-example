package com.flink.example.table.table;

import com.common.example.bean.WordCount;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

/**
 * 功能：Table 转 Stream 示例
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/4/17 下午4:14
 */
public class Table2StreamExample {
    public static void main(String[] args) throws Exception {
        // 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 示例1 toDataStream
        DataStream<WordCount> sourceStream1 = env.fromElements(
                new WordCount("hello", 1),
                new WordCount("word", 4),
                new WordCount("hello", 1));
        Table table1 = tEnv.fromDataStream(sourceStream1);
        DataStream<Row> resultStream1 = tEnv.toDataStream(table1);
        resultStream1.print("R1");

        // 示例2 toAppendStream
        DataStream<WordCount> sourceStream2 = env.fromElements(
                new WordCount("hello", 1),
                new WordCount("word", 4),
                new WordCount("hello", 1));
        Table table2 = tEnv.fromDataStream(sourceStream2);
        DataStream<Row> resultStream2 = tEnv.toAppendStream(table2, Row.class);
        //DataStream<WordCount> resultStream2 = tEnv.toAppendStream(table2, WordCount.class);
        resultStream2.print("R2");

        // 示例3 toRetractStream
        DataStream<WordCount> sourceStream3 = env.fromElements(
                new WordCount("hello", 1),
                new WordCount("word", 4),
                new WordCount("hello", 1));
        tEnv.createTemporaryView("source_table_3", sourceStream3, $("word"), $("frequency"));
        Table table3 = tEnv.sqlQuery("SELECT word, SUM(frequency) AS frequency FROM source_table_3 GROUP BY word");
        DataStream<Tuple2<Boolean, Row>> resultStream3 = tEnv.toRetractStream(table3, Row.class);
        // DataStream<Tuple2<Boolean, WordCount>> resultStream3 = tEnv.toRetractStream(table3, WordCount.class);
        resultStream3.print("R3");

        // 示例4 toRetractStream
        DataStream<WordCount> sourceStream4 = env.fromElements(
                new WordCount("hello", 1),
                new WordCount("word", 4),
                new WordCount("hello", 1));
        tEnv.createTemporaryView("source_table_4", sourceStream4, $("word"), $("frequency"));
        Table table4 = tEnv.sqlQuery("SELECT word, SUM(frequency) FROM source_table_4 GROUP BY word");
        DataStream<Row> resultStream4 = tEnv.toChangelogStream(table4);
        resultStream4.print("R4");

        env.execute();
    }
}
//R2:4> +I[hello, 1]
//R2:2> +I[hello, 1]
//R2:1> +I[word, 4]
//R1:3> +I[hello, 1]
//R1:4> +I[word, 4]
//R1:1> +I[hello, 1]
//R4:3> +I[hello, 1]
//R4:1> +I[word, 4]
//R3:3> (true,+I[hello, 1])
//R4:3> -U[hello, 1]
//R4:3> +U[hello, 2]
//R3:3> (false,-U[hello, 1])
//R3:3> (true,+U[hello, 2])
//R3:1> (true,+I[word, 4])