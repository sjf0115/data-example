package com.spark.example.streaming.window;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * 功能：滑动窗口示例
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2022/11/1 下午11:43
 */
public class SlidingWindowExample {
    private static String hostName = "localhost";
    private static int port = 9100;

    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setAppName("tumble-window-example").setMaster("local[2]");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);

        // 10s一个批次
        JavaStreamingContext ssc = new JavaStreamingContext(sparkContext, Durations.seconds(10));

        // 以端口 9100 作为输入源创建 DStream
        JavaReceiverInputDStream<String> lines = ssc.socketTextStream(hostName, port);

        // 将每行文本切分为单词
        JavaPairDStream<String, Integer> wordCounts = lines.window(Durations.seconds(60), Durations.seconds(10)) // 1分钟的滑动窗口 滑动步长 10s
                .flatMap(new FlatMapFunction<String, String>() {
                    @Override
                    public Iterator<String> call(String x) {
                        return Arrays.asList(x.split("\\s+")).iterator();
                    }
                })
                .mapToPair(new PairFunction<String, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(String word) {
                        return new Tuple2<>(word, 1);
                    }
                })
                .reduceByKey(new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer count1, Integer count2) {
                        return count1 + count2;
                    }
                });
        // 输出
        wordCounts.print();

        // 启动计算
        ssc.start();
        // 等待流计算完成，来防止应用退出
        ssc.awaitTermination();
    }
}
