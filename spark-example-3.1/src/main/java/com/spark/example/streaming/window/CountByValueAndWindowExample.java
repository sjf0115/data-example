package com.spark.example.streaming.window;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

/**
 * 功能：滚动窗口 countByValueAndWindow 算子
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2022/11/1 下午11:21
 */
public class CountByValueAndWindowExample {
    private static String hostName = "localhost";
    private static int port = 9100;

    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setAppName("CountByValueAndWindowExample").setMaster("local[2]");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);

        // 10s一个批次
        JavaStreamingContext ssc = new JavaStreamingContext(sparkContext, Durations.seconds(10));
        // 设置 Checkpoint 路径
        ssc.checkpoint("hdfs://localhost:9000/spark/checkpoint");

        // 以端口 9100 作为输入源创建 DStream
        JavaReceiverInputDStream<String> dStream = ssc.socketTextStream(hostName, port);

        // 数字流 每一分钟计算窗口内元素个数
        JavaPairDStream<Integer, Long> stream = dStream
                .map(number -> Integer.parseInt(number))
                // 窗口大小、滑动间隔
                .countByValueAndWindow(Durations.minutes(1), Durations.minutes(1));

        // 输出
        stream.print();

        // 启动计算
        ssc.start();
        // 等待流计算完成，来防止应用退出
        ssc.awaitTermination();
    }
}
