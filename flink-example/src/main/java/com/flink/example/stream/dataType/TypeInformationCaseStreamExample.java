package com.flink.example.stream.dataType;

import com.common.example.bean.WordCount;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * 功能：TypeInformation 使用场景 Table 转 Stream
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/4/17 下午4:39
 */
public class TypeInformationCaseStreamExample {
    public static void main(String[] args) throws Exception {
        // 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 读取数据创建 DataStream
        DataStream<WordCount> dataStream =
                env.fromElements(
                        new WordCount("Hello", 4),
                        new WordCount("World", 6),
                        new WordCount("Hello", 10)
                );
        Table table = tEnv.fromDataStream(dataStream);

        // 转化为 Pojo 类型
        DataStream<WordCount> stream1 = tEnv.toAppendStream(table, Types.POJO(WordCount.class));
        stream1.print("R1");

        // 转换为 Row 类型
        DataStream<Row> stream2 = tEnv.toAppendStream(table,
                Types.ROW(Types.STRING, Types.LONG)
        );
        stream2.print("R2");

        env.execute();
    }
}
//R1:3> WordCount{word='Hello', frequency=4}
//R1:1> WordCount{word='Hello', frequency=10}
//R1:4> WordCount{word='World', frequency=6}
//R2:2> +I[World, 6]
//R2:1> +I[Hello, 4]
//R2:3> +I[Hello, 10]