package com.flink.example.stream.connector.kafka.offset;

import com.common.example.bean.WordCount;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Properties;

/**
 * 功能：ON_CHECKPOINTS 模式
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2023/6/20 08:06
 */
public class OffsetOnCheckpointsModeExample {
    private static final Logger LOG = LoggerFactory.getLogger(NoCheckpointDisabledExample.class);
    private static final Gson gson = new GsonBuilder().create();

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 开启 Checkpoint 用于容错 每30s触发一次Checkpoint 实际不用设置的这么大
        env.enableCheckpointing(30*1000);
        // 配置失败重启策略：失败后最多重启3次 每次重启间隔10s
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 10000));

        // Kafka Consumer 配置
        Properties consumerProps = new Properties();
        consumerProps.put("bootstrap.servers", "localhost:9092");
        consumerProps.put("group.id", "word-count");

        // 创建 Kafka Consumer
        String consumerTopic = "word";
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(consumerTopic, new SimpleStringSchema(), consumerProps);
        consumer.setStartFromLatest();
        consumer.setCommitOffsetsOnCheckpoints(false);
        DataStreamSource<String> sourceStream = env.addSource(consumer);

        // 单词计数
        DataStream<WordCount> wordCountStream = sourceStream.map(new MapFunction<String, WordCount>() {
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
                })
                .keyBy(wc -> wc.getWord())
                .sum("frequency");

        wordCountStream.print();
        env.execute("OffsetOnCheckpointsModeExample");
    }
}
// a
// b
// a
// Checkpoint
// a
// c
// ERROR