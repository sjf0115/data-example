package com.spark.example.streaming.connector.socket;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.regex.Pattern;

/**
 * 功能：Spark 第一个程序 SocketWordCount
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2022/10/9 下午12:06
 */
public class SocketWordCountLambda {
    private static final Pattern SPACE = Pattern.compile("\\s+");
    private static String hostName = "localhost";
    private static int port = 9100;

    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setAppName("socket-spark-stream").setMaster("local[2]");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);
        JavaStreamingContext ssc = new JavaStreamingContext(sparkContext, Durations.seconds(10));

        JavaReceiverInputDStream<String> lines = ssc.socketTextStream(hostName, port, StorageLevels.MEMORY_AND_DISK_SER);
        JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(SPACE.split(x)).iterator());
        JavaPairDStream<String, Integer> wordCounts = words
                .mapToPair(s -> new Tuple2<>(s, 1))
                .reduceByKey((i1, i2) -> i1 + i2);
        wordCounts.print();

        ssc.start();
        ssc.awaitTermination();
    }
}
