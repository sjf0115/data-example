package com.spark.example.streaming.connector.mysql;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;

/**
 * 功能：与外部存储系统交互 反例 出现 object not serializable 异常
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2024/12/17 22:46
 */
public class DataBaseSinkRddDriverExample {
    private static final Logger LOG = LoggerFactory.getLogger(DataBaseSinkRddDriverExample.class);
    private static String hostName = "localhost";
    private static int port = 9100;

    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setAppName("database-sink-rdd-driver-stream").setMaster("local[2]");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);
        JavaStreamingContext ssc = new JavaStreamingContext(sparkContext, Durations.seconds(10));

        // 以端口 9100 作为输入源创建 DStream
        JavaReceiverInputDStream<String> wordStream = ssc.socketTextStream(hostName, port);

        wordStream.foreachRDD(new VoidFunction<JavaRDD<String>>() {
            @Override
            public void call(JavaRDD<String> rdd) throws Exception {
                // 1. 通过连接池获取连接
                DruidDataSource dataSource = DruidConfig.getDataSource();
                DruidPooledConnection connection = dataSource.getConnection(); // 在 Driver 上执行
                // 2. 遍历 RDD 通过连接与外部存储系统交互
                rdd.foreach(new VoidFunction<String>() {
                    @Override
                    public void call(String record) throws Exception {
                        String[] params = record.split(",");
                        String sql = "INSERT INTO tb_user (id, name, age, email) VALUES (?, ?, ?, ?)";
                        PreparedStatement stmt = null;
                        try {
                            stmt = connection.prepareStatement(sql); // 在 Worker 上执行
                            // 设置参数并执行插入操作
                            stmt.setInt(1, Integer.parseInt(params[0]));
                            stmt.setString(2, params[1]);
                            stmt.setInt(3, Integer.parseInt(params[2]));
                            stmt.setString(4, params[3]);
                            stmt.executeUpdate();
                        } catch (Exception e) {
                            LOG.error("与外部存储系统交互失败：" + e.getMessage());
                        } finally {
                            if (stmt != null) {
                                stmt.close();
                            }
                        }
                    }
                });
                // 3. 关闭连接
                if(connection != null) {
                    connection.close();
                }
            }
        });

        ssc.start();
        ssc.awaitTermination();
    }
}
