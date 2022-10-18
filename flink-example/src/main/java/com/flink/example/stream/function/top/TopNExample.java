package com.flink.example.stream.function.top;

import com.common.example.bean.ShopSales;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * 功能：TopN示例
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2022/10/18 上午8:45
 */
public class TopNExample {

    private static final Gson gson = new GsonBuilder().create();

    public static void main(String[] args) {
        // 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        // 状态后端
        env.setStateBackend(new HashMapStateBackend());
        // 开启 Checkpoint
        env.enableCheckpointing(10000);
        String checkpointPath = "hdfs://localhost:9000/flink/checkpoint";
        env.getCheckpointConfig().setCheckpointStorage(new FileSystemCheckpointStorage(checkpointPath));

        // 配置 Kafka Consumer
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "word-count");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "latest");
        String topic = "shop_sales";
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), props);
        // Kafka Source
        DataStream<String> source = env.addSource(consumer).uid("KafkaSource");
        source.map(new MapFunction<String, ShopSales>() {
            @Override
            public ShopSales map(String value) throws Exception {
                ShopSales shopSales = gson.fromJson(value, ShopSales.class);
                return shopSales;
            }
        }).keyBy(new KeySelector<ShopSales, String>() {
            @Override
            public String getKey(ShopSales shopSales) throws Exception {
                return shopSales.getCategory();
            }
        });
    }
}
