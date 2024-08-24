package com.spark.example.streaming.checkpoint;

import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
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
import java.util.regex.Pattern;

/**
 * 功能：从 kafka 中读取单词进行单词计数 实现 Checkpoint
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2022/10/9 下午12:06
 */
public class KafkaCheckpointWordCount {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaCheckpointWordCount.class);
    private static final Pattern SPACE = Pattern.compile("\\s+");

    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setAppName("KafkaCheckpointWordCount").setMaster("local[*]");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);
        JavaStreamingContext ssc = new JavaStreamingContext(sparkContext, Durations.seconds(10));
        // 设置 Checkpoint 路径
        ssc.checkpoint("hdfs://localhost:9000/spark/checkpoint");

        // Kafka 配置
        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("group.id", "KafkaCheckpointWordCount");
        kafkaParams.put("auto.offset.reset", "largest");
        Set<String> topics = new HashSet<>();
        topics.add("word");

        // 从 Kafka 中读取单词流
        JavaPairInputDStream<String, String> source = KafkaUtils.createDirectStream(ssc,
                String.class, String.class, StringDecoder.class, StringDecoder.class,
                kafkaParams, topics
        );
        // 设置 Checkpoint 周期
        source.checkpoint(Durations.seconds(60));

        // 状态描述符 计算单词个数
        StateSpec<String, Integer, Integer, Tuple2<String, Integer>> stateSpec = StateSpec.function(new Function3<String, Optional<Integer>, State<Integer>, Tuple2<String, Integer>>() {
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
        });

        // 文本行
        JavaDStream<String> lines = source.map(Tuple2::_2);
        // 拆分为单词
        JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(SPACE.split(x)).iterator());
        // 统计每个单词出现的次数
        JavaMapWithStateDStream<String, Integer, Integer, Tuple2<String, Integer>> wordCountStream = words.mapToPair(s -> new Tuple2<>(s, 1))
                .mapWithState(stateSpec);
        // 输出
        wordCountStream.print();

        ssc.start();
        ssc.awaitTermination();
    }
}
