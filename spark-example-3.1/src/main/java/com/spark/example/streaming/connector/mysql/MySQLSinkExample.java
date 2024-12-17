package com.spark.example.streaming.connector.mysql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

/**
 * 功能：输出到 MySQL
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2024/12/17 22:46
 */
public class MySQLSinkExample {
    private static String hostName = "localhost";
    private static int port = 9100;

    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setAppName("mysql-sink-stream").setMaster("local[2]");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);
        JavaStreamingContext ssc = new JavaStreamingContext(sparkContext, Durations.seconds(10));


        // 以端口 9100 作为输入源创建 DStream
        JavaReceiverInputDStream<String> wordStream = ssc.socketTextStream(hostName, port);

        wordStream.foreachRDD(new VoidFunction<JavaRDD<String>>() {
            @Override
            public void call(JavaRDD<String> rdd) throws Exception {
                // 遍历 RDD
                rdd.foreach(new VoidFunction<String>() {
                    @Override
                    public void call(String s) throws Exception {

                    }
                });
            }
        });


        ssc.start();
        ssc.awaitTermination();
    }

    private void createConnection() {

    }
}
