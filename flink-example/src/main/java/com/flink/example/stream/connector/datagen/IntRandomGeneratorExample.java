package com.flink.example.stream.connector.datagen;

import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.datagen.DataGeneratorSource;
import org.apache.flink.streaming.api.functions.source.datagen.RandomGenerator;

/**
 * 功能：简单 Int 随机数生成器 Source 示例
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/3/5 下午10:45
 */
public class IntRandomGeneratorExample {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.disableOperatorChaining();
        env.setParallelism(1);

        // 简单随机生成器 Source
        RandomGenerator<Integer> integerRandomGenerator = RandomGenerator.intGenerator(10001, 99999);
        DataGeneratorSource<Integer> generatorSource = new DataGeneratorSource<>(integerRandomGenerator);

        // 执行
        SingleOutputStreamOperator<Integer> source = env.addSource(generatorSource, "DataGeneratorSource")
                .returns(Integer.class);
        // 输出
        source.print();
        env.execute("IntRandomGeneratorExample");
    }
}
