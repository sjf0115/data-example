package com.spark.example.streaming.conncetor.kafka;

import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.*;
import java.util.regex.Pattern;

/**
 * 功能：基于 Receiver 方式 消费 Kafka 数据
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2022/10/9 下午8:33
 */
public class KafkaReceiverExample {

    private static final Pattern SPACE = Pattern.compile("\\s+");

    public static void main(String[] args) throws Exception {
        SparkConf sparkConf = new SparkConf().setAppName("KafkaReceiverExample");
        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, new Duration(10000));

        // Topic
        int numThreads = 1;
        Map<String, Integer> topics = new HashMap<>();
        topics.put("word", numThreads);

        // Kafka 配置
        int numStreams = 1; // 2 个 Receiver
        String quorum = "localhost:2181";
        String group = "word";

        // Kafka 配置参数
        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("zookeeper.connect", "localhost:2181");
        kafkaParams.put("group.id", "word");
        kafkaParams.put("auto.offset.reset", "largest");

        List<JavaPairDStream<String, String>> kafkaStreams = new ArrayList<>(numStreams);
        for (int i = 0; i < numStreams; i++) {
            //kafkaStreams.add(KafkaUtils.createStream(ssc, quorum, group, topicMap));
            kafkaStreams.add(
                    KafkaUtils.createStream(
                            ssc, String.class, String.class, StringDecoder.class, StringDecoder.class,
                            kafkaParams, topics, StorageLevel.MEMORY_AND_DISK_SER()
                    )
            );
        }
        JavaPairDStream<String, String> source = ssc.union(kafkaStreams.get(0), kafkaStreams.subList(1, kafkaStreams.size()));

        // 文本行
        JavaDStream<String> lines = source.map(new Function<Tuple2<String, String>, String>() {
            @Override
            public String call(Tuple2<String, String> tuple2) throws Exception {
                System.out.println("--------------------------------------------");
                return tuple2._2;
            }
        });
        // 拆分为单词
        JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(SPACE.split(x)).iterator());
        // 统计每个单词出现的次数
        JavaPairDStream<String, Integer> wordCounts = words.mapToPair(s -> new Tuple2<>(s, 1))
                .reduceByKey((i1, i2) -> i1 + i2);
        // 输出
        wordCounts.print();

        ssc.start();
        ssc.awaitTermination();
    }
}
