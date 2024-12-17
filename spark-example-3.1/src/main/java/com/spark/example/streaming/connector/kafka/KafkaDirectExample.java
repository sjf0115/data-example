package com.spark.example.streaming.connector.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.*;
import java.util.regex.Pattern;

/**
 * 功能：kafka 集成 Direct 模式
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2022/10/10 下午7:18
 */
public class KafkaDirectExample {
    private static final Pattern SPACE = Pattern.compile("\\s+");

    public static void main(String[] args) throws Exception {
        SparkConf sparkConf = new SparkConf().setAppName("KafkaDirectExample").setMaster("local[*]");
        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, new Duration(10000));

        // Kafka 配置参数
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "word-3.1.3-direct");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);

        // Topic
        Collection<String> topics = Arrays.asList("spark-words");

        // 输入
        JavaInputDStream<ConsumerRecord<String, String>> source =
                KafkaUtils.createDirectStream(
                        ssc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.Subscribe(topics, kafkaParams)
                );
        JavaPairDStream<String, String> stream = source.mapToPair(record -> new Tuple2<>(record.key(), record.value()));

        // 文本行
        JavaDStream<String> lines = stream.map(Tuple2::_2);
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
