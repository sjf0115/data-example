package com.flink.example.stream.connector.redis;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * 功能：Flink JedisPool
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2022/10/28 下午10:04
 */
public class FlinkJedisPool implements FlinkRedisCommand, Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(FlinkJedisPool.class);
    private transient JedisPool jedisPool;

    private FlinkJedisPool(JedisPool jedisPool) {
        Objects.requireNonNull(jedisPool, "Jedis Pool can not be null");
        this.jedisPool = jedisPool;
    }

    @Override
    public void open() throws Exception {

    }

    /**
     * 获取实例
     * @return
     */
    private Jedis getInstance() {
        return this.jedisPool.getResource();
    }

    /**
     * 释放实例
     * @param jedis
     */
    private void releaseInstance(Jedis jedis) {
        if(jedis != null) {
            try {
                jedis.close();
            } catch (Exception var3) {
                LOG.error("Failed to close (return) instance to pool", var3);
            }

        }
    }

    @Override
    public void hset(String key, String hashField, String value, Integer ttl) {
        Jedis jedis = null;
        try {
            jedis = this.getInstance();
            jedis.hset(key, hashField, value);
            if(ttl != null) {
                jedis.expire(key, ttl.intValue());
            }
        } catch (Exception var10) {
            if(LOG.isErrorEnabled()) {
                LOG.error("Cannot send Redis message with command HSET to key {} and hashField {} error message {}", new Object[]{key, hashField, var10.getMessage()});
            }
            throw var10;
        } finally {
            this.releaseInstance(jedis);
        }
    }

    @Override
    public String hget(String key, String fieldKey) {
        Jedis jedis = null;
        try {
            jedis = this.getInstance();
            return jedis.hget(key, fieldKey);
        } catch (Exception var10) {
            if(LOG.isErrorEnabled()) {
                LOG.error("Cannot send Redis message with command HGET to key {} and fieldKey {} error message {}", key, fieldKey, var10.getMessage());
            }
            throw var10;
        } finally {
            this.releaseInstance(jedis);
        }
    }

    @Override
    public Map<String, String> hgetAll(String key) {
        Jedis jedis = null;
        try {
            jedis = this.getInstance();
            return jedis.hgetAll(key);
        } catch (Exception var10) {
            if(LOG.isErrorEnabled()) {
                LOG.error("Cannot send Redis message with command HGETALL to key {} error message {}", key, var10.getMessage());
            }
            throw var10;
        } finally {
            this.releaseInstance(jedis);
        }
    }

    @Override
    public void hincrBy(String key, String hashField, Long value, Integer ttl) {
        Jedis jedis = null;
        try {
            jedis = this.getInstance();
            jedis.hincrBy(key, hashField, value.longValue());
            if(ttl != null) {
                jedis.expire(key, ttl.intValue());
            }
        } catch (Exception var10) {
            if(LOG.isErrorEnabled()) {
                LOG.error("Cannot send Redis message with command HINCRBY to key {} and hashField {} error message {}", new Object[]{key, hashField, var10.getMessage()});
            }
            throw var10;
        } finally {
            this.releaseInstance(jedis);
        }
    }

    @Override
    public void rpush(String listName, String value) {
        Jedis jedis = null;
        try {
            jedis = this.getInstance();
            jedis.rpush(listName, new String[]{value});
        } catch (Exception var8) {
            if(LOG.isErrorEnabled()) {
                LOG.error("Cannot send Redis message with command RPUSH to list {} error message {}", listName, var8.getMessage());
            }
            throw var8;
        } finally {
            this.releaseInstance(jedis);
        }
    }

    @Override
    public void lpush(String listName, String value) {
        Jedis jedis = null;
        try {
            jedis = this.getInstance();
            jedis.lpush(listName, new String[]{value});
        } catch (Exception var8) {
            if(LOG.isErrorEnabled()) {
                LOG.error("Cannot send Redis message with command LUSH to list {} error message {}", listName, var8.getMessage());
            }
            throw var8;
        } finally {
            this.releaseInstance(jedis);
        }
    }

    @Override
    public void sadd(String setName, String value) {
        Jedis jedis = null;
        try {
            jedis = this.getInstance();
            jedis.sadd(setName, new String[]{value});
        } catch (Exception var8) {
            if(LOG.isErrorEnabled()) {
                LOG.error("Cannot send Redis message with command RPUSH to set {} error message {}", setName, var8.getMessage());
            }
            throw var8;
        } finally {
            this.releaseInstance(jedis);
        }
    }

    @Override
    public void publish(String channelName, String message) {
        Jedis jedis = null;
        try {
            jedis = this.getInstance();
            jedis.publish(channelName, message);
        } catch (Exception var8) {
            if(LOG.isErrorEnabled()) {
                LOG.error("Cannot send Redis message with command PUBLISH to channel {} error message {}", channelName, var8.getMessage());
            }
            throw var8;
        } finally {
            this.releaseInstance(jedis);
        }
    }

    @Override
    public void set(String key, String value) {
        Jedis jedis = null;
        try {
            jedis = this.getInstance();
            jedis.set(key, value);
        } catch (Exception var8) {
            if(LOG.isErrorEnabled()) {
                LOG.error("Cannot send Redis message with command SET to key {} error message {}", key, var8.getMessage());
            }
            throw var8;
        } finally {
            this.releaseInstance(jedis);
        }
    }

    @Override
    public String get(String key) {
        Jedis jedis = null;
        try {
            jedis = this.getInstance();
            return jedis.get(key);
        } catch (Exception var8) {
            if(LOG.isErrorEnabled()) {
                LOG.error("Cannot send Redis message with command GET to key {} error message {}", key, var8.getMessage());
            }
            throw var8;
        } finally {
            this.releaseInstance(jedis);
        }
    }

    @Override
    public void setex(String key, String value, Integer ttl) {
        Jedis jedis = null;
        try {
            jedis = this.getInstance();
            jedis.setex(key, ttl.intValue(), value);
        } catch (Exception var9) {
            if(LOG.isErrorEnabled()) {
                LOG.error("Cannot send Redis message with command SETEX to key {} error message {}", key, var9.getMessage());
            }
            throw var9;
        } finally {
            this.releaseInstance(jedis);
        }
    }

    @Override
    public void setbit(String key, Long offset, boolean value) {
        Jedis jedis = null;
        try {
            jedis = this.getInstance();
            jedis.setbit(key, offset, value);
        } catch (Exception var8) {
            if(LOG.isErrorEnabled()) {
                LOG.error("Cannot send Redis message with command SET to key {} error message {}", key, var8.getMessage());
            }
            throw var8;
        } finally {
            this.releaseInstance(jedis);
        }
    }

    @Override
    public long bitcount(String key) {
        Jedis jedis = null;
        try {
            jedis = this.getInstance();
            long count = jedis.bitcount(key);
            return count;
        } catch (Exception var8) {
            if(LOG.isErrorEnabled()) {
                LOG.error("Cannot send Redis message with command SET to key {} error message {}", key, var8.getMessage());
            }
            throw var8;
        } finally {
            this.releaseInstance(jedis);
        }
    }

    @Override
    public void pfadd(String key, String element) {
        Jedis jedis = null;
        try {
            jedis = this.getInstance();
            jedis.pfadd(key, new String[]{element});
        } catch (Exception var8) {
            if(LOG.isErrorEnabled()) {
                LOG.error("Cannot send Redis message with command PFADD to key {} error message {}", key, var8.getMessage());
            }
            throw var8;
        } finally {
            this.releaseInstance(jedis);
        }
    }

    @Override
    public long pfcount(String key) {
        Jedis jedis = null;
        try {
            jedis = this.getInstance();
            long count = jedis.pfcount(key);
            return count;
        } catch (Exception var8) {
            if(LOG.isErrorEnabled()) {
                LOG.error("Cannot send Redis message with command PFADD to key {} error message {}", key, var8.getMessage());
            }
            throw var8;
        } finally {
            this.releaseInstance(jedis);
        }
    }

    @Override
    public void zadd(String key, String score, String element) {
        Jedis jedis = null;
        try {
            jedis = this.getInstance();
            jedis.zadd(key, Double.valueOf(score).doubleValue(), element);
        } catch (Exception var9) {
            if(LOG.isErrorEnabled()) {
                LOG.error("Cannot send Redis message with command ZADD to set {} error message {}", key, var9.getMessage());
            }
            throw var9;
        } finally {
            this.releaseInstance(jedis);
        }
    }

    @Override
    public void zincrBy(String key, String score, String element) {
        Jedis jedis = null;
        try {
            jedis = this.getInstance();
            jedis.zincrby(key, Double.valueOf(score).doubleValue(), element);
        } catch (Exception var9) {
            if(LOG.isErrorEnabled()) {
                LOG.error("Cannot send Redis message with command ZINCRBY to set {} error message {}", key, var9.getMessage());
            }
            throw var9;
        } finally {
            this.releaseInstance(jedis);
        }
    }

    @Override
    public void zrem(String key, String element) {
        Jedis jedis = null;
        try {
            jedis = this.getInstance();
            jedis.zrem(key, new String[]{element});
        } catch (Exception var8) {
            if(LOG.isErrorEnabled()) {
                LOG.error("Cannot send Redis message with command ZREM to set {} error message {}", key, var8.getMessage());
            }
            throw var8;
        } finally {
            this.releaseInstance(jedis);
        }
    }

    @Override
    public void incrByEx(String key, Long value, Integer ttl) {
        Jedis jedis = null;
        try {
            jedis = this.getInstance();
            jedis.incrBy(key, value.longValue());
            if(ttl != null) {
                jedis.expire(key, ttl.intValue());
            }
        } catch (Exception var9) {
            if(LOG.isErrorEnabled()) {
                LOG.error("Cannot send Redis with incrby command to key {} with increment {}  with ttl {} error message {}", new Object[]{key, value, ttl, var9.getMessage()});
            }
            throw var9;
        } finally {
            this.releaseInstance(jedis);
        }
    }

    @Override
    public void decrByEx(String key, Long value, Integer ttl) {
        Jedis jedis = null;
        try {
            jedis = this.getInstance();
            jedis.decrBy(key, value.longValue());
            if(ttl != null) {
                jedis.expire(key, ttl.intValue());
            }
        } catch (Exception var9) {
            if(LOG.isErrorEnabled()) {
                LOG.error("Cannot send Redis with decrBy command to key {} with decrement {}  with ttl {} error message {}", new Object[]{key, value, ttl, var9.getMessage()});
            }
            throw var9;
        } finally {
            this.releaseInstance(jedis);
        }
    }

    @Override
    public void incrBy(String key, Long value) {
        Jedis jedis = null;
        try {
            jedis = this.getInstance();
            jedis.incrBy(key, value.longValue());
        } catch (Exception var8) {
            if(LOG.isErrorEnabled()) {
                LOG.error("Cannot send Redis with incrby command to key {} with increment {}  error message {}", new Object[]{key, value, var8.getMessage()});
            }
            throw var8;
        } finally {
            this.releaseInstance(jedis);
        }
    }

    @Override
    public void decrBy(String key, Long value) {
        Jedis jedis = null;
        try {
            jedis = this.getInstance();
            jedis.decrBy(key, value.longValue());
        } catch (Exception var8) {
            if(LOG.isErrorEnabled()) {
                LOG.error("Cannot send Redis with decrBy command to key {} with increment {}  error message {}", new Object[]{key, value, var8.getMessage()});
            }
            throw var8;
        } finally {
            this.releaseInstance(jedis);
        }
    }

    @Override
    public void close() throws IOException {
        if(this.jedisPool != null) {
            this.jedisPool.close();
        }
    }

    public static FlinkJedisPool build(FlinkJedisPoolConfig jedisPoolConfig) {
        Preconditions.checkNotNull(jedisPoolConfig, "Redis pool config should not be Null");

        String host = jedisPoolConfig.getHost();
        int port = jedisPoolConfig.getPort();
        int connectionTimeout = jedisPoolConfig.getConnectionTimeout();
        String password = jedisPoolConfig.getPassword();
        int database = jedisPoolConfig.getDatabase();
        int maxIdle = jedisPoolConfig.getMaxIdle();
        int maxTotal = jedisPoolConfig.getMaxTotal();
        int minIdle = jedisPoolConfig.getMinIdle();
        boolean testWhileIdle = jedisPoolConfig.isTestWhileIdle();
        boolean testOnBorrow = jedisPoolConfig.isTestOnBorrow();
        boolean testOnReturn = jedisPoolConfig.isTestOnReturn();

        GenericObjectPoolConfig genericObjectPoolConfig;
        if (testWhileIdle) {
            genericObjectPoolConfig = new JedisPoolConfig();
        } else {
            genericObjectPoolConfig =  new GenericObjectPoolConfig();
        }
        genericObjectPoolConfig.setMaxIdle(maxIdle);
        genericObjectPoolConfig.setMaxTotal(maxTotal);
        genericObjectPoolConfig.setMinIdle(minIdle);
        genericObjectPoolConfig.setTestOnBorrow(testOnBorrow);
        genericObjectPoolConfig.setTestOnReturn(testOnReturn);

        JedisPool jedisPool = new JedisPool(genericObjectPoolConfig, host, port, connectionTimeout, password, database);
        return new FlinkJedisPool(jedisPool);
    }
}
