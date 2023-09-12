package com.spark.example.core.base;

import avro.shaded.com.google.common.collect.Lists;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

/**
 * 功能：共享变量-广播变量
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2023/9/13 07:30
 */
public class BroadcastVariablesExample {
    private static final Logger LOG = LoggerFactory.getLogger(BroadcastVariablesExample.class);
    private static String hostName = "localhost";
    private static int port = 9100;

    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setAppName("BroadcastVariablesExample").setMaster("local[2]");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        List<String> blacklist = Lists.newArrayList("a", "b", "d", "f");
        // 对变量 blacklist 封装为广播变量
        Broadcast<List<String>> blacklistBC = jsc.broadcast(blacklist);
        LOG.info("黑名单：{}", blacklistBC.value());

        JavaStreamingContext ssc = new JavaStreamingContext(jsc, Durations.seconds(10));
        // 以端口 9100 作为输入源创建 DStream
        JavaReceiverInputDStream<String> lines = ssc.socketTextStream(hostName, port);

        lines.flatMap(line -> Arrays.asList(line.split("\\s+")).iterator())
                .filter(new Function<String, Boolean>() {
                    @Override
                    public Boolean call(String word) throws Exception {
                        List<String> blackList = blacklistBC.value();
                        if (blackList.contains(word)) {
                            LOG.info("{} 命中黑名单被过滤", word);
                            return false;
                        } else {
                            LOG.info("{} 未命中黑名单", word);
                            return true;
                        }
                    }
                })
                .print();

        // 启动计算
        ssc.start();
        // 等待流计算完成，来防止应用退出
        ssc.awaitTermination();
    }
}
