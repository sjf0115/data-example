package com.flink.example.stream.checkpoint;

import com.flink.example.bean.WBehavior;
import com.flink.example.stream.function.BehaviorParseMapFunction;
import com.flink.example.stream.function.BehaviorSumMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * FsStateBackend
 * Created by wy on 2020/12/9.
 */
public class FsStateBackendExample {
    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "metric-group");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "latest");

        String topic = "weibo_behavior";
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), props);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(60000);
        env.setStateBackend(new FsStateBackend("hdfs://localhost:9000/flink/stateBackend"));
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        env.getCheckpointConfig().setCheckpointTimeout(60000);

        DataStreamSource<String> source = env.addSource(consumer).setParallelism(1);
        source.map(new BehaviorParseMapFunction()).setParallelism(1).uid("behavior-parse-map-function")
                .keyBy(WBehavior::getUid)
                .map(new BehaviorSumMapFunction()).setParallelism(1).uid("behavior-sum-map-function")
                .print();

        env.execute("fsStateBackend-example");
    }
}
