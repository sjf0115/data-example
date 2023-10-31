package com.spark.example.streaming.checkpoint;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function0;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.util.LongAccumulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * 功能：使用 Checkpoint 实现状态可恢复的 WordCount
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2023/9/11 07:29
 */
public class SocketRecoverableWordCount {
    private static final Logger LOG = LoggerFactory.getLogger(SocketRecoverableWordCount.class);
    private static String hostName = "localhost";
    private static int port = 9100;

    public static void main(String[] args) throws InterruptedException {
        String checkpointDirectory = "hdfs://localhost:9000/spark/checkpoint";

        JavaStreamingContext context = JavaStreamingContext.getOrCreate(checkpointDirectory, new Function0<JavaStreamingContext>() {
            @Override
            public JavaStreamingContext call() throws Exception {
                SparkConf conf = new SparkConf().setAppName("SocketRecoverableWordCount").setMaster("local[2]");
                JavaSparkContext sparkContext = new JavaSparkContext(conf);
                JavaStreamingContext ssc = new JavaStreamingContext(sparkContext, Durations.seconds(20));

                ssc.checkpoint(checkpointDirectory);

                JavaReceiverInputDStream<String> lines = ssc.socketTextStream(hostName, port);
                JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(x.split("\\s+")).iterator());
                JavaPairDStream<String, Integer> wordCounts = words
                        .mapToPair(s -> new Tuple2<>(s, 1))
                        .reduceByKey((i1, i2) -> i1 + i2);

                wordCounts.foreachRDD((rdd, time) -> {
                    // 注册 Broadcast 黑名单
                    Broadcast<List<String>> excludeList =
                            JavaWordExcludeList.getInstance(new JavaSparkContext(rdd.context()));
                    // 注册 Accumulator 记录排除黑名单中的元素个数
                    LongAccumulator droppedWordsCounter =
                            JavaDroppedWordsCounter.getInstance(new JavaSparkContext(rdd.context()));
                    // 记录丢失的单词个数
                    String counts = rdd.filter(wordCount -> {
                        if (excludeList.value().contains(wordCount._1())) {
                            droppedWordsCounter.add(wordCount._2());
                            return false;
                        } else {
                            return true;
                        }
                    }).collect().toString();
                    LOG.info("........................Counts at time {} {}", time, counts);
                    LOG.info("........................Dropped {} word(s) totally", droppedWordsCounter.value());
                });

                wordCounts.print();
                return ssc;
            }
        });
        context.start();
        context.awaitTermination();
    }
}

// 广播
class JavaWordExcludeList {

    private static volatile Broadcast<List<String>> instance = null;

    public static Broadcast<List<String>> getInstance(JavaSparkContext jsc) {
        if (instance == null) {
            synchronized (JavaWordExcludeList.class) {
                if (instance == null) {
                    List<String> wordExcludeList = Arrays.asList("a", "b", "c");
                    instance = jsc.broadcast(wordExcludeList);
                }
            }
        }
        return instance;
    }
}

// 计数器
class JavaDroppedWordsCounter {

    private static volatile LongAccumulator instance = null;

    public static LongAccumulator getInstance(JavaSparkContext jsc) {
        if (instance == null) {
            synchronized (JavaDroppedWordsCounter.class) {
                if (instance == null) {
                    instance = jsc.sc().longAccumulator("DroppedWordsCounter");
                }
            }
        }
        return instance;
    }
}