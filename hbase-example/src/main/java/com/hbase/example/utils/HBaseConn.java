package com.hbase.example.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

/**
 * HBase 连接工具类
 * Created by wy on 2020/1/12.
 */
public class HBaseConn {

    // 单例
    private static final HBaseConn INSTANCE = new HBaseConn();
    private static Configuration config;
    private static Connection conn;

    private HBaseConn() {
        try {
            if (config == null) {
                config = HBaseConfiguration.create();
                config.set("hbase.zookeeper.quorum", "127.0.0.1:2181");
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取连接
     * @return
     */
    private Connection getConnection() {
        if (conn == null || conn.isClosed()) {
            try {
                conn = ConnectionFactory.createConnection(config);
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        }
        return conn;
    }

    /**
     * 关闭连接
     */
    private void closeConnection() {
        if (conn != null) {
            try {
                conn.close();
            }
            catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 获取连接
     * @return
     */
    public static Connection create() {
        return INSTANCE.getConnection();
    }

    /**
     * 关闭连接
     */
    public static void close() {
        INSTANCE.closeConnection();
    }
}
