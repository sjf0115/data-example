package com.spark.example.streaming.conncetor.kafka;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
    private static final Logger LOG = LoggerFactory.getLogger(KafkaReceiverExample.class);

    private static final Pattern SPACE = Pattern.compile("\\s+");

    public static void main(String[] args) throws Exception {

        SparkConf conf = new SparkConf().setAppName("KafkaReceiverExample").setMaster("local[2]");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);
        sparkContext.setLogLevel("WARN");
        JavaStreamingContext ssc = new JavaStreamingContext(sparkContext, Durations.seconds(10));

        // Topic
        int numThreads = 1;
        Map<String, Integer> topics = new HashMap<>();
        topics.put("spark-words", numThreads);

        // Kafka 配置
        int numStreams = 1; // 2 个 Receiver
        String quorum = "localhost:2181";
        String group = "spark-words-streaming";

        // Kafka 配置参数
        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("zookeeper.connect", quorum);
        kafkaParams.put("group.id", group);
        kafkaParams.put("auto.offset.reset", "largest");

//        List<JavaPairDStream<String, String>> kafkaStreams = new ArrayList<>(numStreams);
//        for (int i = 0; i < numStreams; i++) {
//            //kafkaStreams.add(KafkaUtils.createStream(ssc, quorum, group, topicMap));
//            kafkaStreams.add(
//                    KafkaUtils.createStream(
//                            ssc, String.class, String.class, StringDecoder.class, StringDecoder.class,
//                            kafkaParams, topics, StorageLevel.MEMORY_AND_DISK_SER()
//                    )
//            );
//        }
//        JavaPairDStream<String, String> source = ssc.union(kafkaStreams.get(0), kafkaStreams.subList(1, kafkaStreams.size()));


//        JavaPairReceiverInputDStream<String, String> source = KafkaUtils.createStream(
//                ssc, String.class, String.class, StringDecoder.class, StringDecoder.class,
//                kafkaParams, topics, StorageLevel.MEMORY_AND_DISK_SER()
//        );

        JavaPairReceiverInputDStream<String, String> source =
                KafkaUtils.createStream(ssc, quorum, group, topics);

        // 文本行
        JavaDStream<String> lines = source.map(new Function<Tuple2<String, String>, String>() {
            @Override
            public String call(Tuple2<String, String> tuple2) throws Exception {
                LOG.info("输入 key: {}, value: {}", tuple2._1(), tuple2._2());
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
