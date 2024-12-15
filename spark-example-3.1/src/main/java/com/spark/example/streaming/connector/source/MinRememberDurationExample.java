package com.spark.example.streaming.connector.source;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

/**
 * 功能：Source - textFileStream - spark.streaming.minRememberDuration
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2024/12/8 09:48
 */
public class MinRememberDurationExample {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf()
                .setAppName("text-file-stream")
                .setMaster("local[2]")
                .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")  ;

        // 设置记忆窗口为1个小时 即文件修改时间戳在最近一个小时内就可以被处理
        conf.set("spark.streaming.minRememberDuration", "6000000s");

        JavaSparkContext sparkContext = new JavaSparkContext(conf);
        JavaStreamingContext ssc = new JavaStreamingContext(sparkContext, Durations.seconds(10));

        String path = "hdfs://localhost:9000/user/hive/warehouse/tag_user";

        // 过滤临时文件
        Function<Path, Boolean> filter = new Function<Path, Boolean>() {
            @Override
            public Boolean call(Path path) throws Exception {
                return !path.getName().startsWith(".");
            }
        };

        // 读取文件
        JavaPairInputDStream<LongWritable, Text> dStream = ssc.fileStream(
                path,
                LongWritable.class,
                Text.class,
                TextInputFormat.class,
                filter,
                false
        );
        dStream.print();

        ssc.start();
        ssc.awaitTermination();
    }
}
