package com.flink.example.stream.app.uv;

import com.common.example.utils.RoaringBitmapUtil;
import com.flink.example.stream.connector.redis.FlinkJedisPool;
import com.flink.example.stream.connector.redis.FlinkJedisPoolConfig;
import jodd.util.StringUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * 功能：RedisConnectorExample
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2022/10/27 下午11:38
 */
public class RedisRoaringBitmapDAU {

    private static final Logger LOG = LoggerFactory.getLogger(RedisRoaringBitmapDAU.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStream<String> source = env.socketTextStream("localhost", 9100, "\n")
                .setParallelism(1).uid("socket-source");

        // <dt, os, uid>
        SingleOutputStreamOperator<Tuple3<String, String, Long>> userStream = source
                .map(new MapFunction<String, Tuple3<String, String, Long>>() {
                    @Override
                    public Tuple3<String, String, Long> map(String str) throws Exception {
                        String[] params = str.split(",");
                        String dt = params[0];
                        String os = params[1];
                        Long uid = Long.parseLong(params[2]);
                        return Tuple3.of(dt, os, uid);
                    }
                });

        FlinkJedisPoolConfig config = new FlinkJedisPoolConfig.Builder()
                .setHost("localhost")
                .setPort(6379)
                .build();

        userStream.addSink(new RedisRoaringBitmapSink(config));

        env.execute();
    }

    // RoaringBitmap
    public static class RedisRoaringBitmapSink extends RichSinkFunction<Tuple3<String, String, Long>> {
        private FlinkJedisPool jedisPool;
        private FlinkJedisPoolConfig config;
        public RedisRoaringBitmapSink(FlinkJedisPoolConfig config) {
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
        public void invoke(Tuple3<String, String, Long> element, Context context) throws Exception {
            String dt = element.f0;
            String os = element.f1;
            Long uid = element.f2;
            // Key 为 dt, field key 为 os, field value 为 RoaringBitmap

            // 操作系统维度
            String osBitmapValue = jedisPool.hget(dt, os);
            Roaring64NavigableMap osBitmap = new Roaring64NavigableMap();
            if (StringUtils.isNotBlank(osBitmapValue)) {
                osBitmap = RoaringBitmapUtil.parseBitmap(osBitmapValue);
            }
            if (!osBitmap.contains(uid)) {
                osBitmap.addLong(uid);
                String newOsValue = RoaringBitmapUtil.parseString(osBitmap);
                jedisPool.hset(dt, os, newOsValue, 24*60*60*2);
            }
            long osUV = osBitmap.getLongCardinality();
            LOG.info("dt: {}, os: {}, uid: {}, uv: {}", dt, os, uid, osUV);

            // 整体维度
            Map<String, String> valueMap = jedisPool.hgetAll(dt);
            Roaring64NavigableMap bitmap = new Roaring64NavigableMap();
            for (String fieldKey : valueMap.keySet()) {
                String value = valueMap.get(fieldKey);
                if (StringUtil.isBlank(value)) {
                    continue;
                }
                Roaring64NavigableMap fieldBitmap = RoaringBitmapUtil.parseBitmap(value);
                bitmap.or(fieldBitmap);
            }
            long totalUV = bitmap.getLongCardinality();
            LOG.info("dt: {}, os: 全部, uid: {}, uv: {}", dt, uid, totalUV);
        }

        @Override
        public void close() throws IOException {
            if (jedisPool != null) {
                jedisPool.close();
            }
        }
    }
}
//20221001,Android,10001
//20221001,Android,10002
//20221001,Android,10001
//20221001,iOS,10003
//20221001,Android,10004
//20221001,Android,10002
//20221001,iOS,10003
//20221001,iOS,10005
//20221001,Android,10006
//20221002,Android,10001
//20221002,iOS,10003
//20221002,Android,10001
//20221002,Android,10006