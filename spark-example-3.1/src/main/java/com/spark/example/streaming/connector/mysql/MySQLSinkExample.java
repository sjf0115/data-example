package com.spark.example.streaming.connector.mysql;

import com.alibaba.druid.pool.DruidDataSource;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * 功能：输出到 MySQL
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2024/12/17 22:46
 */
public class MySQLSinkExample {
    private static final Logger LOG = LoggerFactory.getLogger(MySQLSinkExample.class);
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
                LOG.info("foreachRDD 遍历 RDD ...");
                if (rdd.isEmpty()) {
                    return;
                }

                // 获取Druid数据源
                DruidDataSource dataSource = DruidConfig.getDataSource();

                // 遍历 RDD
                rdd.foreach(new VoidFunction<String>() {
                    @Override
                    public void call(String record) throws Exception {
                        LOG.info("foreach 遍历 RDD 中每条记录 ...");
                        String[] params = record.split(",");
                        try (Connection connection = dataSource.getConnection();
                             PreparedStatement stmt = connection.prepareStatement("INSERT INTO tb_user (id, name, age, email) VALUES (?, ?, ?, ?)")) {
                            // 设置参数并执行插入操作
                            stmt.setInt(1, Integer.parseInt(params[0]));
                            stmt.setString(2, params[1]);
                            stmt.setInt(3, Integer.parseInt(params[2]));
                            stmt.setString(4, params[3]);
                            stmt.executeUpdate();
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }
                    }
                });
            }
        });

        ssc.start();
        ssc.awaitTermination();
    }
}
