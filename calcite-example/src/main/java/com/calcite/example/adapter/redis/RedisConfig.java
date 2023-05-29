package com.calcite.example.adapter.redis;

/**
 * 功能：Redis 配置类
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2023/5/29 上午7:34
 */
public class RedisConfig {
    private final String host;
    private final int port;
    private final int database;
    private final String password;

    public RedisConfig(String host, int port, int database, String password) {
        this.host = host;
        this.port = port;
        this.database = database;
        this.password = password;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public int getDatabase() {
        return database;
    }

    public String getPassword() {
        return password;
    }

    @Override
    public String toString() {
        return "RedisConfig{" +
                "host='" + host + '\'' +
                ", port=" + port +
                ", database=" + database +
                ", password='" + password + '\'' +
                '}';
    }
}
