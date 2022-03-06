package com.flink.example.stream.source.datagen;

import com.flink.example.bean.Order;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.datagen.DataGeneratorSource;
import org.apache.flink.streaming.api.functions.source.datagen.SequenceGenerator;

/**
 * 功能：
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/3/5 下午10:48
 */
public class SequenceGeneratorExample {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.disableOperatorChaining();
        env.setParallelism(1);

        // 复杂序列生成器 自己实现Next逻辑
        SequenceGenerator<Order> sequenceGenerator = new SequenceGenerator<Order>(10001, 99999) {
            RandomDataGenerator random = new RandomDataGenerator();
            @Override
            public Order next() {
                return new Order(
                        StringUtils.upperCase(random.nextSecureHexString(8)),
                        // 递增
                        valuesToEmit.poll().intValue(),
                        random.nextUniform(1, 1000),
                        System.currentTimeMillis()
                );
            }
        };
        DataGeneratorSource<Order> generatorSource = new DataGeneratorSource<>(sequenceGenerator, 1L, 5L);

        // 执行
        SingleOutputStreamOperator<Order> source = env.addSource(generatorSource, "DataGeneratorSource")
                .returns(Types.POJO(Order.class));
        // 输出
        source.print();
        env.execute("SequenceGeneratorExample");
    }
}
