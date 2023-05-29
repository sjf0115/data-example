package com.calcite.example.adapter.redis;

import com.google.common.cache.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Protocol;

import static java.util.Objects.requireNonNull;

/**
 * 功能：
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2023/5/29 上午7:38
 */
public class RedisJedisManager implements AutoCloseable {
    private final LoadingCache<String, JedisPool> jedisPoolCache;
    private final JedisPoolConfig jedisPoolConfig;

    private final int maxTotal = GenericObjectPoolConfig.DEFAULT_MAX_TOTAL;
    private final int maxIdle = GenericObjectPoolConfig.DEFAULT_MAX_IDLE;
    private final int minIdle = GenericObjectPoolConfig.DEFAULT_MIN_IDLE;
    private final int timeout = Protocol.DEFAULT_TIMEOUT;

    private final String host;
    private final String password;
    private final int port;
    private final int database;

    public RedisJedisManager(RedisConfig config) {
        this(config.getHost(), config.getPort(), config.getDatabase(), config.getPassword());
    }

    public RedisJedisManager(String host, int port, int database, String password) {
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxTotal(maxTotal);
        jedisPoolConfig.setMaxIdle(maxIdle);
        jedisPoolConfig.setMinIdle(minIdle);
        this.host = host;
        this.port = port;
        this.database = database;
        this.password = password;
        this.jedisPoolConfig = jedisPoolConfig;
        this.jedisPoolCache = CacheBuilder.newBuilder()
                .removalListener(new JedisPoolRemovalListener())
                .build(CacheLoader.from(this::createConsumer));
    }

    public JedisPool getJedisPool() {
        requireNonNull(host, "host is null");
        return jedisPoolCache.getUnchecked(host);
    }

    public Jedis getResource() {
        return getJedisPool().getResource();
    }

    private JedisPool createConsumer() {
        String pwd = password;
        if (StringUtils.isEmpty(pwd)) {
            pwd = null;
        }
        return new JedisPool(jedisPoolConfig, host, port, timeout, pwd, database);
    }

    private static class JedisPoolRemovalListener implements RemovalListener<String, JedisPool> {
        @Override public void onRemoval(RemovalNotification<String, JedisPool> notification) {
            assert notification.getValue() != null;
            try {
                notification.getValue().destroy();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Override public void close() {
        jedisPoolCache.invalidateAll();
    }
}
