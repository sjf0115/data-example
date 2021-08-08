package com.flink.example.stream.connector;

import com.flink.example.bean.Behavior;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * 读取Kafka写入Kafka
 * Created by wy on 2020/10/18.
 */
public class KafkaToKafkaStream {

    private static final Gson gson = new GsonBuilder().create();

    public static void main(String[] args) throws Exception{
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        //props.put("zookeeper.connect", "localhost:2181");
        props.put("group.id", "metric-group");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "latest");

        String topic = "behavior";
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), props);

        SingleOutputStreamOperator<String> behavior = env.addSource(consumer)
                .setParallelism(1)
                .map(new MapFunction<String, String>() {
                    @Override
                    public String map(String s) throws Exception {
                        String[] params = s.split("\\s+");
                        int size = params.length;
                        Behavior behavior = new Behavior();
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
                        String json = gson.toJson(behavior);
                        return json;
                    }
                }); //

        behavior.print();

        env.execute("flink-kafka-stream");
    }
}
