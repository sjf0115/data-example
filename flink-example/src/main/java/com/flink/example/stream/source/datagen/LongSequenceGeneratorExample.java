package com.flink.example.stream.source.datagen;

import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.datagen.DataGeneratorSource;
import org.apache.flink.streaming.api.functions.source.datagen.SequenceGenerator;

/**
 * 功能：简单 Long 序列生成器 Source 示例
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/3/5 下午11:09
 */
public class LongSequenceGeneratorExample {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.disableOperatorChaining();
        env.setParallelism(1);

        // 简单序列生成器
        SequenceGenerator<Long> simpleSequenceGenerator = SequenceGenerator.longGenerator(10001, 99999);
        DataGeneratorSource<Long> generatorSource = new DataGeneratorSource<>(simpleSequenceGenerator);

        // 执行
        SingleOutputStreamOperator<Long> source = env.addSource(generatorSource, "DataGeneratorSource")
                .returns(Long.class);
        // 输出
        source.print();
        env.execute("LongSequenceGeneratorExample");
    }
}
