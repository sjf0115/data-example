package com.spark.example.streaming.checkpoint;

import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function0;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.*;

/**
 * 功能：从 Kafka 中读取单词流 实现故障恢复 从 Checkpoint 中恢复
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2024/8/24 19:08
 */
public class KafkaRecoverableWordCount {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaRecoverableWordCount.class);

    public static void main(String[] args) throws InterruptedException {
        String checkpointDirectory = "hdfs://localhost:9000/spark/checkpoint";

        JavaStreamingContext context = JavaStreamingContext.getOrCreate(checkpointDirectory, new Function0<JavaStreamingContext>() {
            @Override
            public JavaStreamingContext call() throws Exception {
                SparkConf conf = new SparkConf().setAppName("KafkaRecoverableWordCount").setMaster("local[*]");
                JavaSparkContext sparkContext = new JavaSparkContext(conf);
                JavaStreamingContext ssc = new JavaStreamingContext(sparkContext, Durations.seconds(30));
                // 设置 Checkpoint 路径
                ssc.checkpoint(checkpointDirectory);

                // Kafka 配置
                Map<String, String> kafkaParams = new HashMap<>();
                kafkaParams.put("bootstrap.servers", "localhost:9092");
                kafkaParams.put("group.id", "KafkaRecoverableWordCount");
                kafkaParams.put("auto.offset.reset", "largest");
                Set<String> topics = new HashSet<>();
                topics.add("word");

                // 从 Kafka 中读取单词流
                JavaPairInputDStream<String, String> source = KafkaUtils.createDirectStream(ssc,
                        String.class, String.class, StringDecoder.class, StringDecoder.class,
                        kafkaParams, topics
                );
                // 设置 Checkpoint 周期
                source.checkpoint(Durations.seconds(90));

                // 单词计数
                JavaDStream<String> lines = source.map(new Function<Tuple2<String, String>, String>() {
                    @Override
                    public String call(Tuple2<String, String> tuple2) throws Exception {
                        String word = tuple2._2();
                        LOG.info("word: {}", word);
                        // 失败信号 模拟作业遇到脏数据
                        if (Objects.equals(word, "ERROR")) {
                            throw new RuntimeException("custom error flag, restart application");
                        }
                        return word;
                    }
                });

                JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(x.split("\\s+")).iterator());
                JavaMapWithStateDStream<String, Integer, Integer, Tuple2<String, Integer>> wordCounts = words
                        .mapToPair(s -> new Tuple2<>(s, 1))
                        .mapWithState(StateSpec.function(new Function3<String, Optional<Integer>, State<Integer>, Tuple2<String, Integer>>() {
                            @Override
                            public Tuple2<String, Integer> call(String word, Optional<Integer> count, State<Integer> countState) throws Exception {
                                LOG.info("Word: {}, Count: {}, State: {}", word, count.or(0), countState.exists() ? countState.get() : null);
                                int newCount = 0;
                                if (countState.exists()) {
                                    newCount = countState.get();
                                }
                                newCount += count.orElse(0);
                                if (!countState.isTimingOut()) {
                                    countState.update(newCount);
                                }
                                return new Tuple2<>(word, newCount);
                            }
                        }));

                wordCounts.print();

                return ssc;
            }
        });
        context.start();
        context.awaitTermination();
    }
}