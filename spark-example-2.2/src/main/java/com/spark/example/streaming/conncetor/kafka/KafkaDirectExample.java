package com.spark.example.streaming.conncetor.kafka;

import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.*;
import java.util.regex.Pattern;

/**
 * 功能：
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2022/10/10 下午7:18
 */
public class KafkaDirectExample {
    private static final Pattern SPACE = Pattern.compile("\\s+");

    public static void main(String[] args) throws Exception {
        SparkConf sparkConf = new SparkConf().setAppName("KafkaDirectExample");
        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, new Duration(10000));

        // Kafka 配置参数
        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("group.id", "word");
        kafkaParams.put("auto.offset.reset", "largest");

        // Topic
        Set<String> topics = new HashSet<>();
        topics.add("word");

        JavaPairInputDStream<String, String> source = KafkaUtils.createDirectStream(ssc,
                String.class, String.class, StringDecoder.class, StringDecoder.class,
                kafkaParams, topics
        );

        // 文本行
        JavaDStream<String> lines = source.map(Tuple2::_2);
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
