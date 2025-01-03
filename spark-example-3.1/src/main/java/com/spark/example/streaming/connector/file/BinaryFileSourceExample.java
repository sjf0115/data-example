package com.spark.example.streaming.connector.file;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

/**
 * 功能：Source - binaryRecordsStream
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2024/12/8 09:48
 */
public class BinaryFileSourceExample {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setAppName("text-file-stream").setMaster("local[2]");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);
        JavaStreamingContext ssc = new JavaStreamingContext(sparkContext, Durations.seconds(10));

        String path = "hdfs://localhost:9000/user/hive/warehouse/tag_user";
        JavaDStream<byte[]> dStream = ssc.binaryRecordsStream(path, 100);
        dStream.foreachRDD(
                new VoidFunction<JavaRDD<byte[]>>() {
                    @Override
                    public void call(JavaRDD<byte[]> rdd) throws Exception {
                        Object[] objects = rdd.collect().toArray();
                        for (Object object : objects) {
                            System.out.println(object.toString());
                        }
                    }
                }
            );

        ssc.start();
        ssc.awaitTermination();
    }
}
