package com.spark.example.streaming.state;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.JavaMapWithStateDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * 功能：使用 MapWithState 实现有状态的 WordCount
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2022/10/9 下午12:06
 */
public class SocketMapStateWordCount {
    private static String hostName = "localhost";
    private static int port = 9100;

    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setAppName("SocketMapStateWordCount").setMaster("local[2]");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);

        JavaStreamingContext ssc = new JavaStreamingContext(sparkContext, Durations.seconds(10));

        // 通过 updateStateByKey 实现有状态的应用必须实现 Checkpoint
        ssc.checkpoint("hdfs://localhost:9000/spark/checkpoint");

        // 以端口 9100 作为输入源创建 DStream
        JavaReceiverInputDStream<String> lines = ssc.socketTextStream(hostName, port);


        // 用来初始化 State
        List<Tuple2<String, Integer>> elements = Arrays.asList(
                new Tuple2("hello", 1),
                new Tuple2<>("world", 1)
        );
        JavaPairRDD<String, Integer> elementRDD = ssc.sparkContext().parallelizePairs(elements);

        // 统计每个单词出现的次数
        JavaMapWithStateDStream<String, Integer, Integer, Tuple2<String, Integer>> wordCounts = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String x) {
                return Arrays.asList(x.split("\\s+")).iterator();
            }
        }).mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String word) {
                return new Tuple2<>(word, 1);
            }
        }).mapWithState(
                StateSpec.function(new Function3<String, Optional<Integer>, State<Integer>, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> call(String word, Optional<Integer> count, State<Integer> countState) throws Exception {
                        int newCount = 0;
                        if (countState.exists()) {
                            newCount = countState.get();
                        }
                        newCount += count.orElse(0);
                        countState.update(newCount);
                        return new Tuple2<>(word, newCount);
                    }
                }).initialState(elementRDD)
        );

        // 输出
        wordCounts.print();

        // 启动计算
        ssc.start();
        // 等待流计算完成，来防止应用退出
        ssc.awaitTermination();
    }
}
