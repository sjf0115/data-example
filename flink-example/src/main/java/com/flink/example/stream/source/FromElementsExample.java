package com.flink.example.stream.source;

import com.google.common.collect.Lists;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FromElementsFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.List;

/**
 * 功能：FromElementsFunction 示例
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/3/6 下午6:27
 */
public class FromElementsExample {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.disableOperatorChaining();
        env.setParallelism(1);

        // 方式1
        DataStreamSource<String> source = env.fromElements("a", "b", "c", "d");
        source.print("1");

        // 方式2
        List<String> list = Lists.newArrayList("a", "b", "c", "d");
        DataStreamSource<String> source1 = env.fromCollection(list);
        source1.print("2");

        // 方式3
        SourceFunction<String> function = new FromElementsFunction<>("a", "b", "c", "d");
        SingleOutputStreamOperator<String> source2 = env.addSource(function, "FromElements")
                .returns(String.class);
        source2.print("3");

        // 执行
        env.execute("FromElementsExample");
    }
}
