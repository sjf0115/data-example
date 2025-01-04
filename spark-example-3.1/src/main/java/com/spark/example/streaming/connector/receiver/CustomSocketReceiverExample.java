package com.spark.example.streaming.connector.receiver;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

/**
 * 功能：CustomSocketReceiver 示例
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2025/1/4 17:47
 */
public class CustomSocketReceiverExample {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setAppName("CustomSocketReceiverExample").setMaster("local[2]");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);
        JavaStreamingContext ssc = new JavaStreamingContext(sparkContext, Durations.seconds(10));

        // 自定义 Receiver
        CustomSocketReceiver receiver = new CustomSocketReceiver("localhost", 9100);
        // 集成到数据流中
        JavaDStream<String> dStream = ssc.receiverStream(receiver);
        dStream.print();

        ssc.start();
        ssc.awaitTermination();
    }
}
