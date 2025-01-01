package com.spark.example.streaming.window;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.util.Arrays;
import java.util.Iterator;

/**
 * 功能：滚动窗口 reduceByWindow 算子
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2022/11/1 下午11:21
 */
public class ReduceByWindowExample {
    private static String hostName = "localhost";
    private static int port = 9100;

    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setAppName("ReduceByWindowExample").setMaster("local[2]");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);

        // 10s一个批次
        JavaStreamingContext ssc = new JavaStreamingContext(sparkContext, Durations.seconds(10));

        // 以端口 9100 作为输入源创建 DStream
        JavaReceiverInputDStream<String> numberStream = ssc.socketTextStream(hostName, port);

        // 数字流 每一分钟计算数字和
        JavaDStream<Integer> stream = numberStream
                .map(number -> Integer.parseInt(number))
                .reduceByWindow(
                        // reduce 聚合函数
                        new Function2<Integer, Integer, Integer>() {
                            @Override
                            public Integer call(Integer v1, Integer v2) {
                                return v1 + v2;
                            }
                        },
                        // 窗口大小
                        Durations.minutes(1),
                        // 滑动间隔
                        Durations.minutes(1)
                );

        // 输出
        stream.print();

        // 启动计算
        ssc.start();
        // 等待流计算完成，来防止应用退出
        ssc.awaitTermination();
    }
}
