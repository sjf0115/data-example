package com.spark.example.streaming.connector.receiver;

import com.common.example.random.RandomGenerator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

/**
 * 功能：DataGenerator
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2024/12/8 09:48
 */
public class DataGeneratorExample {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setAppName("DataGeneratorExample").setMaster("local[2]");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);
        JavaStreamingContext ssc = new JavaStreamingContext(sparkContext, Durations.seconds(10));

        RandomGenerator<String> randomGenerator = RandomGenerator.stringGenerator(5);
        JavaDStream<String> dStream = ssc.receiverStream(new DataGeneratorReceiver(randomGenerator, 1, 100L));
        dStream.print();

        ssc.start();
        ssc.awaitTermination();
    }
}
