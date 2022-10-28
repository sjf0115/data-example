package com.flink.example.stream.connector.redis;

import scala.Serializable;

import java.util.Objects;

/**
 * 功能：FlinkJedisPoolConfig
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2022/10/28 下午9:51
 */
public class FlinkJedisPoolConfig implements Serializable {
    private static final long serialVersionUID = 1L;
    private final String host;
    private final int port;
    private final int database;
    private final int maxTotal;
    private final int maxIdle;
    private final int minIdle;
    private final int connectionTimeout;
    private final String password;
    private final boolean testOnBorrow;
    private final boolean testOnReturn;
    private final boolean testWhileIdle;

    private FlinkJedisPoolConfig(String host, int port, int connectionTimeout, String password,
                                 int database, int maxTotal, int maxIdle, int minIdle,
                                 boolean testOnBorrow, boolean testOnReturn, boolean testWhileIdle) {
        Objects.requireNonNull(host, "Host information should be presented");
        this.host = host;
        this.port = port;
        this.database = database;
        this.connectionTimeout = connectionTimeout;
        this.maxTotal = maxTotal;
        this.maxIdle = maxIdle;
        this.minIdle = minIdle;
        this.password = password;
        this.testOnBorrow = testOnBorrow;
        this.testOnReturn = testOnReturn;
        this.testWhileIdle = testWhileIdle;
    }

    public String getHost() {
        return this.host;
    }

    public int getPort() {
        return this.port;
    }

    public int getDatabase() {
        return this.database;
    }


    public int getMaxTotal() {
        return maxTotal;
    }

    public int getMaxIdle() {
        return maxIdle;
    }

    public int getMinIdle() {
        return minIdle;
    }

    public int getConnectionTimeout() {
        return connectionTimeout;
    }

    public String getPassword() {
        return password;
    }

    public boolean isTestOnBorrow() {
        return testOnBorrow;
    }

    public boolean isTestOnReturn() {
        return testOnReturn;
    }

    public boolean isTestWhileIdle() {
        return testWhileIdle;
    }

    @Override
    public String toString() {
        return "FlinkJedisPoolConfig{" +
                "host='" + host + '\'' +
                ", port=" + port +
                ", database=" + database +
                ", maxTotal=" + maxTotal +
                ", maxIdle=" + maxIdle +
                ", minIdle=" + minIdle +
                ", connectionTimeout=" + connectionTimeout +
                ", password='" + password + '\'' +
                ", testOnBorrow=" + testOnBorrow +
                ", testOnReturn=" + testOnReturn +
                ", testWhileIdle=" + testWhileIdle +
                '}';
    }

    public static class Builder {
        private String host;
        private int port = 6379;
        private int timeout = 2000;
        private int database = 0;
        private String password;
        private int maxTotal = 8;
        private int maxIdle = 8;
        private int minIdle = 0;
        private boolean testOnBorrow = false;
        private boolean testOnReturn = false;
        private boolean testWhileIdle = false;

        public Builder() {
        }

        public FlinkJedisPoolConfig.Builder setMaxTotal(int maxTotal) {
            this.maxTotal = maxTotal;
            return this;
        }

        public FlinkJedisPoolConfig.Builder setMaxIdle(int maxIdle) {
            this.maxIdle = maxIdle;
            return this;
        }

        public FlinkJedisPoolConfig.Builder setMinIdle(int minIdle) {
            this.minIdle = minIdle;
            return this;
        }

        public FlinkJedisPoolConfig.Builder setHost(String host) {
            this.host = host;
            return this;
        }

        public FlinkJedisPoolConfig.Builder setPort(int port) {
            this.port = port;
            return this;
        }

        public FlinkJedisPoolConfig.Builder setTimeout(int timeout) {
            this.timeout = timeout;
            return this;
        }

        public FlinkJedisPoolConfig.Builder setDatabase(int database) {
            this.database = database;
            return this;
        }

        public FlinkJedisPoolConfig.Builder setPassword(String password) {
            this.password = password;
            return this;
        }

        public FlinkJedisPoolConfig.Builder setTestOnBorrow(boolean testOnBorrow) {
            this.testOnBorrow = testOnBorrow;
            return this;
        }

        public FlinkJedisPoolConfig.Builder setTestOnReturn(boolean testOnReturn) {
            this.testOnReturn = testOnReturn;
            return this;
        }

        public FlinkJedisPoolConfig.Builder setTestWhileIdle(boolean testWhileIdle) {
            this.testWhileIdle = testWhileIdle;
            return this;
        }

        public FlinkJedisPoolConfig build() {
            return new FlinkJedisPoolConfig(this.host, this.port, this.timeout, this.password, this.database,
                    this.maxTotal, this.maxIdle, this.minIdle,
                    this.testOnBorrow, this.testOnReturn, testWhileIdle);
        }
    }
}
