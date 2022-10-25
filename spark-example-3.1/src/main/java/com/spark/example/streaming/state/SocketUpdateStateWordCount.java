package com.spark.example.streaming.state;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
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
import java.util.List;

/**
 * 功能：使用 UpdateStateByKey 实现有状态的 WordCount
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2022/10/9 下午12:06
 */
public class SocketUpdateStateWordCount {
    private static String hostName = "localhost";
    private static int port = 9100;

    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setAppName("SocketUpdateStateWordCount").setMaster("local[2]");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);

        JavaStreamingContext ssc = new JavaStreamingContext(sparkContext, Durations.seconds(10));

        // 通过 updateStateByKey 实现有状态的应用必须实现 Checkpoint
        ssc.checkpoint("hdfs://localhost:9000/spark/checkpoint");

        // 以端口 9100 作为输入源创建 DStream
        JavaReceiverInputDStream<String> lines = ssc.socketTextStream(hostName, port);

        // 统计每个单词出现的次数
        JavaPairDStream<String, Long> wordCounts = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String x) {
                return Arrays.asList(x.split("\\s+")).iterator();
            }
        }).mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String word) {
                return new Tuple2<>(word, 1);
            }
        }).updateStateByKey(new Function2<List<Integer>, Optional<Long>, Optional<Long>>() {
            // List<Integer> 表示当前批次接收到的指定 Key 的所有值
            // Optional<Long> 表示存储类型为 Long 的状态
            @Override
            public Optional<Long> call(List<Integer> elements, Optional<Long> countState) throws Exception {
                Long count = 0L;
                if (countState.isPresent()) {
                    count = countState.get();
                }
                for (Integer value : elements) {
                    count += value;
                }
                return Optional.of(count);
            }
        });

        // 将此 DStream 中生成的每个RDD的前10个元素打印到控制台
        wordCounts.print();

        // 启动计算
        ssc.start();
        // 等待流计算完成，来防止应用退出
        ssc.awaitTermination();
    }
}
