package com.calcite.example.adapter.redis;

import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.commons.lang3.StringUtils;
import redis.clients.jedis.Jedis;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 功能：Redis 读取迭代器 Enumerator
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2023/5/29 上午12:00
 */
public class RedisEnumerator implements Enumerator<Object[]> {

    private final Enumerator<Object[]> enumerator;

    RedisEnumerator(RedisConfig config, RedisSchema schema, String tableName) {
        RedisTableField tableField = schema.getTableField(tableName);
        RedisJedisManager redisManager = new RedisJedisManager(config);
        // 获取一个Redis实例
        try (Jedis jedis = redisManager.getResource()) {
            if (StringUtils.isNotEmpty(config.getPassword())) {
                jedis.auth(config.getPassword());
            }
            RedisDataProcess dataProcess = new RedisDataProcess(jedis, tableField);
            List<Object[]> objs = dataProcess.read();
            enumerator = Linq4j.enumerator(objs);
        }
    }

    @Override
    public Object[] current() {
        return enumerator.current();
    }

    @Override
    public boolean moveNext() {
        return enumerator.moveNext();
    }

    @Override
    public void reset() {
        enumerator.reset();
    }

    @Override
    public void close() {
        enumerator.close();
    }
}
