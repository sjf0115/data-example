package com.flink.example.stream.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FromIteratorFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.NumberSequenceIterator;

/**
 * 功能：
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/3/6 下午6:51
 */
public class FromIteratorExample {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.disableOperatorChaining();
        env.setParallelism(1);

        // 方式1
        DataStreamSource<Long> source = env.fromCollection(new NumberSequenceIterator(1L, 20L), Long.class);
        source.print("1");

        // 方式2
        SourceFunction<Long> function = new FromIteratorFunction<>(new NumberSequenceIterator(1L, 20L));
        SingleOutputStreamOperator<Long> source1 = env.addSource(function, "FromIterator")
                .returns(Long.class);
        source1.print("2");

        // 执行
        env.execute("FromIteratorExample");
    }
}
