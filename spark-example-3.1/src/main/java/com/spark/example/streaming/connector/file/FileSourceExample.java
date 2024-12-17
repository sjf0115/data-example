package com.spark.example.streaming.connector.file;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

/**
 * 功能：Source - FileStream
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2024/12/14 17:07
 */
public class FileSourceExample {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setAppName("file-stream").setMaster("local[2]");
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
                true
        );
        dStream.print();

        ssc.start();
        ssc.awaitTermination();
    }
}
