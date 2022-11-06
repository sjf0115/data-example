package com.flink.example.stream.connector.redis;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

// Flink Redis 命令
public interface FlinkRedisCommand extends Serializable {
    void open() throws Exception;

    void hset(String key, String hashField, String value, Integer ttl);

    String hget(String key, String fieldKey);

    Map<String, String> hgetAll(String key);

    void hincrBy(String key, String hashField, Long value, Integer ttl);

    void rpush(String listName, String value);

    void lpush(String listName, String value);

    void sadd(String setName, String value);

    void publish(String channelName, String message);

    void set(String key, String value);

    String get(String key);

    void setex(String key, String value, Integer ttl);

    void setbit(String key, Long offset, boolean value);

    long bitcount(String key);

    void pfadd(String key, String element);

    long pfcount(String key);

    void zadd(String key, String score, String element);

    void zincrBy(String key, String score, String element);

    void zrem(String key, String element);

    void incrByEx(String key, Long value, Integer ttl);

    void decrByEx(String key, Long value, Integer ttl);

    void incrBy(String key, Long value);

    void decrBy(String key, Long value);

    void close() throws IOException;
}
