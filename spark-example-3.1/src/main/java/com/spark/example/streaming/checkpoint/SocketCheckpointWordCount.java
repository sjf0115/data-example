package com.spark.example.streaming.checkpoint;

import com.spark.example.bean.SerializableComparator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * 功能：单词计数 实现 Checkpoint
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2022/10/9 下午12:06
 */
public class SocketCheckpointWordCount {
    private static final Logger LOG = LoggerFactory.getLogger(SocketCheckpointWordCount.class);
    private static String hostName = "localhost";
    private static int port = 9100;

    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setAppName("SocketCheckpointWordCount").setMaster("local[*]");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);
        JavaStreamingContext ssc = new JavaStreamingContext(sparkContext, Durations.seconds(10));
        // 设置 Checkpoint 路径
        ssc.checkpoint("hdfs://localhost:9000/spark/checkpoint");
        // 单词流
        JavaReceiverInputDStream<String> lines = ssc.socketTextStream(hostName, port);
        // 设置 Checkpoint 周期
        lines.checkpoint(Durations.seconds(60));

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

        JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(x.split("\\s+")).iterator());
        JavaMapWithStateDStream<String, Integer, Integer, Tuple2<String, Integer>> wordCounts = words
                .mapToPair(s -> new Tuple2<>(s, 1))
                .mapWithState(stateSpec);

        wordCounts.foreachRDD(rdd -> {
            List<Tuple2<String, Integer>> top = rdd.top(10, new SerializableComparator<Tuple2<String, Integer>>() {
                @Override
                public int compare(Tuple2<String, Integer> o1, Tuple2<String, Integer> o2) {
                    long count1 = o1._2;
                    long count2 = o2._2;
                    if (count1 > count2) {
                        return 1;
                    } else if (count1 < count2) {
                        return -1;
                    }
                    return 0;
                }
            });
            top.forEach(tuple -> System.out.println(tuple._1+","+tuple._2));
        });

        ssc.start();
        ssc.awaitTermination();
    }
}
