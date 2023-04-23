package com.flink.example.stream.function.stateful;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * 功能：RuntimeContext 实现操作 KeyedState 的有状态函数
 *         连续两次的温度变化超过阈值则报警
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2023/4/22 下午11:07
 */
public class RuntimeContextExample {
    private static final Logger LOG = LoggerFactory.getLogger(RuntimeContextExample.class);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        // 每10s一次Checkpoint
        env.enableCheckpointing(30 * 1000);

        // Socket 输入
        DataStream<String> stream = env.socketTextStream("localhost", 9100, "\n");

        // 传感器温度流
        DataStream<Tuple3<String, Double, Double>> alertStream = stream.map(new MapFunction<String, Tuple2<String, Double>>() {
            @Override
            public Tuple2<String, Double> map(String value) throws Exception {
                if(Objects.equals(value, "ERROR")) {
                    throw new RuntimeException("error dirty data");
                }
                String[] params = value.split(",");
                LOG.info("sensor input, id: {}, temperature: {}", params[0], params[1]);
                return new Tuple2<>(params[0], Double.parseDouble(params[1]));
            }
        }).keyBy(new KeySelector<Tuple2<String, Double>, String>() {
            @Override
            public String getKey(Tuple2<String, Double> sensor) throws Exception {
                return sensor.f0;
            }
        }).flatMap(new TemperatureAlertFlatMapFunction(10));// 温度变化超过10度则报警
        alertStream.print();

        env.execute("RuntimeContextExample");
    }

    // FlatMap 的好处是在温度变化不超过阈值的时候不进行输出
    public static class TemperatureAlertFlatMapFunction extends RichFlatMapFunction<Tuple2<String, Double>, Tuple3<String, Double, Double>> {
        // 温度差报警阈值
        private double threshold;
        // 上一次温度
        private ValueState<Double> lastTemperatureState;
        public TemperatureAlertFlatMapFunction(double threshold) {
            this.threshold = threshold;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            // 初始化
            ValueStateDescriptor<Double> stateDescriptor = new ValueStateDescriptor<>("lastTemperature", Double.class);
            lastTemperatureState = getRuntimeContext().getState(stateDescriptor);
        }

        @Override
        public void flatMap(Tuple2<String, Double> sensor, Collector<Tuple3<String, Double, Double>> out) throws Exception {
            String sensorId = sensor.f0;
            // 当前温度
            double temperature = sensor.f1;

            // 上一次温度
            Double lastTemperature = this.lastTemperatureState.value();
            // 获取新的温度之后更新上一次的温度
            lastTemperatureState.update(temperature);
            if (Objects.equals(lastTemperature, null)) {
                LOG.info("sensor first temperature, id: {}, temperature: {}", sensorId, temperature);
                return;
            }

            double diff = Math.abs(temperature - lastTemperature);
            if (diff > threshold) {
                // 温度变化超过阈值
                LOG.info("sensor alert, id: {}, temperature: {}, lastTemperature: {}, diff: {}", sensorId, temperature, lastTemperature, diff);
                out.collect(Tuple3.of(sensorId, temperature, diff));
            } else {
                LOG.info("sensor no alert, id: {}, temperature: {}, lastTemperature: {}, diff: {}", sensorId, temperature, lastTemperature, diff);
            }
        }
    }
}
// 1,35.4
// 1,20.8
// 2,23.5
// ERROR
// 1,31.6
// 2,37.2
