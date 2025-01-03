package com.flink.example.stream.connector.redis;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * 功能：RedisSink
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2022/10/29 上午12:13
 */
public class RedisSink extends RichSinkFunction<Tuple2<Long, Integer>> {
    private static final Logger LOG = LoggerFactory.getLogger(RedisSink.class);
    private FlinkJedisPool jedisPool;
    private FlinkJedisPoolConfig config;

    public RedisSink(FlinkJedisPoolConfig config) {
        this.config = config;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        try {
            jedisPool = FlinkJedisPool.build(config);
        } catch (Exception e) {
            LOG.error("Redis has not been properly initialized: ", e);
            throw e;
        }
    }

    @Override
    public void invoke(Tuple2<Long, Integer> element, Context context) throws Exception {
        String bitKey = "uv_bit";
        Long uid = element.f0;
        jedisPool.setbit(bitKey, uid, true);
        long bitCount = jedisPool.bitcount(bitKey);
        LOG.info("uid: {}, bitCount: {},", uid, bitCount);
    }

    @Override
    public void close() throws IOException {
        if (jedisPool != null) {
            jedisPool.close();
        }
    }
}
