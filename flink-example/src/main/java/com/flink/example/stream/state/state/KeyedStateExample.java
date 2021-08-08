package com.flink.example.stream.state.state;

import com.flink.example.bean.Behavior;
import com.flink.example.stream.function.BehaviorParseMapFunction;
import com.flink.example.stream.function.BehaviorSumMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

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
                .keyBy(Behavior::getUid)
                .map(new BehaviorSumMapFunction()).setParallelism(1).uid("behavior-sum-map-function");//

        env.execute("keyed-state-stream");
    }
}
