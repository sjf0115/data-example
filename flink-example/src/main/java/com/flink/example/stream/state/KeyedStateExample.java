package com.flink.example.stream.state;

import com.flink.example.bean.WeiboBehavior;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Objects;
import java.util.Properties;

/**
 * Keyed State Demo
 * Created by wy on 2020/11/17.
 */
public class KeyedStateExample {

    public static void main(String[] args) throws Exception{
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "metric-group");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "latest");

        String topic = "weibo_behavior";
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), props);

        DataStreamSource<String> source = env.addSource(consumer).setParallelism(1);


        DataStream<Long> stream = source.map(new BehaviorParseMapFunction()).setParallelism(1).uid("behavior-parse-map-function")
                .keyBy(WeiboBehavior::getUid)
                .map(new BehaviorSumMapFunction()).setParallelism(1).uid("behavior-sum-map-function");//

        env.execute("keyed-state-stream");
    }

    // 微博行为解析
    public static class BehaviorParseMapFunction extends RichMapFunction<String, WeiboBehavior> {
        @Override
        public WeiboBehavior map(String s) throws Exception {
            String[] params = s.split("\\s+");
            int size = params.length;
            WeiboBehavior behavior = new WeiboBehavior();
            if (size > 0) {
                behavior.setUid(params[0]);
            }
            if (size > 1) {
                behavior.setWid(params[1]);
            }
            if (size > 2) {
                behavior.setTime(params[2]);
            }
            if (size > 3) {
                behavior.setContent(params[3]);
            }
            return behavior;
        }
    }

    // 微博求和解析
    public static class BehaviorSumMapFunction extends RichMapFunction<WeiboBehavior, Long> {

        private ValueState<Long> counterState;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            ValueStateDescriptor<Long> stateDescriptor = new ValueStateDescriptor<>("counter", Long.class);
            counterState = getRuntimeContext().getState(stateDescriptor);
        }

        @Override
        public Long map(WeiboBehavior behavior) throws Exception {
            Long count = counterState.value();
            if (Objects.equals(count, null)) {
                count = 0L;
            }
            Long newCount = count + 1;
            counterState.update(newCount);
            System.out.println(behavior.getUid() + ":" + newCount);
            return newCount;
        }
    }
}
