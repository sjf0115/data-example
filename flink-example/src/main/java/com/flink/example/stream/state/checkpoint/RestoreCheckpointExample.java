package com.flink.example.stream.state.checkpoint;

import com.common.example.bean.WordCount;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.storage.JobManagerCheckpointStorage;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Properties;

/**
 * 从 Checkpoint 中恢复作业
 * Created by wy on 2020/12/26.
 */
public class RestoreCheckpointExample {

    private static final Logger LOG = LoggerFactory.getLogger(RestoreCheckpointExample.class);
    private static Gson gson = new GsonBuilder().create();

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 配置 状态后端
        env.setStateBackend(new HashMapStateBackend());
        // 配置 Checkpoint 每30s触发一次Checkpoint 实际不用设置的这么大
        env.enableCheckpointing(30*1000);
        env.getCheckpointConfig().setCheckpointStorage(new JobManagerCheckpointStorage());

        // 配置失败重启策略：失败后最多重启3次 每次重启间隔10s
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 10000));

        // 配置 Kafka Consumer
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "word-count");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "latest");
        String topic = "word";
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), props);
        // Kafka Source
        DataStream<String> source = env.addSource(consumer).uid("KafkaSource");

        // 计算单词个数
        SingleOutputStreamOperator<WordCount> result = source
                .map(new MapFunction<String, WordCount>() {
                    @Override
                    public WordCount map(String element) throws Exception {
                        WordCount wordCount = gson.fromJson(element, WordCount.class);
                        String word = wordCount.getWord();
                        LOG.info("word: {}, frequency: {}", word, wordCount.getFrequency());
                        // 失败信号 模拟作业遇到脏数据
                        if (Objects.equals(word, "ERROR")) {
                            throw new RuntimeException("custom error flag, restart application");
                        }
                        return wordCount;
                    }
                }).uid("Map")
                .keyBy(new KeySelector<WordCount, String>() {
                    @Override
                    public String getKey(WordCount element) throws Exception {
                        return element.getWord();
                    }
                })
                .sum("frequency").uid("Sum");

        result.print().uid("Print");
        env.execute("RestoreCheckpointExample");
    }
}
// a
// b
// a
// Checkpoint
// a
// c
// ERROR