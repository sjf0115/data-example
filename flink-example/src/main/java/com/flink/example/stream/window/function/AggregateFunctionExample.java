package com.flink.example.stream.window.function;

import com.flink.example.stream.source.simple.SimpleTemperatureSource;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 功能：窗口 AverageAggregateFunction 示例
 *      计算平均温度
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2022/8/28 下午4:20
 */
public class AggregateFunctionExample {
    private static final Logger LOG = LoggerFactory.getLogger(AggregateFunctionExample.class);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000L);

        // Stream of (id, temperature)
        DataStreamSource<Tuple2<String, Integer>> source = env.addSource(new SimpleTemperatureSource());

        // 计算分钟内的平均温度
        SingleOutputStreamOperator<Tuple2<String, Double>> stream = source
                // 分组
                .keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Integer> value) throws Exception {
                        LOG.info("[Source] id: {}, temperature: {}", value.f0, value.f1);
                        return value.f0;
                    }
                })
                // 窗口大小为1分钟的滚动窗口
                .window(TumblingProcessingTimeWindows.of(Time.minutes(1)))
                .aggregate(new AvgTemperatureAggregateFunction());

        stream.print();
        env.execute("AggregateFunctionExample");
    }

    /**
     * 自定义AggregateFunction
     *      IN：Tuple2<String, Integer> 输入类型
     *      ACC：Tuple3<String, Integer, Integer> -> <Key, Sum, Count> 中间结果类型
     *      OUT：Tuple2<String, Double> 输出类型
     */
    private static class AvgTemperatureAggregateFunction implements AggregateFunction<
            Tuple2<String, Integer>,
            Tuple3<String, Integer, Integer>,
            Tuple2<String, Double>> {
        @Override
        public Tuple3<String, Integer, Integer> createAccumulator() {
            // 累加器 Key, Sum, Count
            return new Tuple3<String, Integer, Integer>("", 0, 0);
        }

        @Override
        public Tuple3<String, Integer, Integer> add(Tuple2<String, Integer> value, Tuple3<String, Integer, Integer> accumulator) {
            // 输入一个元素 更新累加器
            return new Tuple3<String, Integer, Integer>(value.f0, accumulator.f1 + value.f1, accumulator.f2 + 1);
        }

        @Override
        public Tuple2<String, Double> getResult(Tuple3<String, Integer, Integer> accumulator) {
            // 从累加器中获取总和和个数计算平均值
            double avgTemperature = ((double) accumulator.f1) / accumulator.f2;
            LOG.info("[AggregateFunction] id: {}, avgTemperature: {}", accumulator.f0, avgTemperature);
            return new Tuple2<String, Double>(accumulator.f0, avgTemperature);
        }

        @Override
        public Tuple3<String, Integer, Integer> merge(Tuple3<String, Integer, Integer> a, Tuple3<String, Integer, Integer> b) {
            // 累加器合并
            return new Tuple3<String, Integer, Integer>(a.f0, a.f1 + b.f1, a.f2 + b.f2);
        }
    }
}
