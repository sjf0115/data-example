package com.flink.example.stream.app.uv;

import com.common.example.utils.RoaringBitmapUtil;
import com.flink.example.stream.connector.redis.FlinkJedisPool;
import com.flink.example.stream.connector.redis.FlinkJedisPoolConfig;
import com.flink.example.stream.connector.redis.RedisSink;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * 功能：Redis DAU 对比
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2022/10/27 下午11:38
 */
public class RedisDAU {

    private static final Logger LOG = LoggerFactory.getLogger(RedisSink.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> source = env.socketTextStream("localhost", 9100, "\n")
                .setParallelism(1).uid("socket-source");

        SingleOutputStreamOperator<Tuple2<Long, Integer>> result = source
                .map(new MapFunction<String, Tuple2<Long, Integer>>() {
                    @Override
                    public Tuple2<Long, Integer> map(String str) throws Exception {
                        Long uid = Long.parseLong(str);
                        return Tuple2.of(uid, 1);
                    }
                });


//        DataStreamSource<LoginUser> source = env.addSource(new UserPressureMockSource()).setParallelism(1);
//        SingleOutputStreamOperator<Tuple2<Long, Integer>> result = source
//                .map(new MapFunction<LoginUser, Tuple2<Long, Integer>>() {
//                    @Override
//                    public Tuple2<Long, Integer> map(LoginUser user) throws Exception {
//                        LOG.info("uid: {}", user.getUid());
//                        return Tuple2.of(user.getUid(), 1);
//                    }
//                });

        FlinkJedisPoolConfig config = new FlinkJedisPoolConfig.Builder()
                .setHost("localhost")
                .setPort(6379)
                .build();

        result.addSink(new RedisDAUSink(config)).setParallelism(1);

        env.execute();
    }

    public static class RedisDAUSink extends RichSinkFunction<Tuple2<Long, Integer>> {
        private FlinkJedisPool jedisPool;
        private FlinkJedisPoolConfig config;

        public RedisDAUSink(FlinkJedisPoolConfig config) {
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
            String hlKey = "uv_hl";
            String bitKey = "uv_bit";
            String bitmapKey = "uv_bitmap";
            Long uid = element.f0;

            jedisPool.setbit(bitKey, uid, true);
            jedisPool.pfadd(hlKey, String.valueOf(uid));

            Roaring64NavigableMap bitmap = new Roaring64NavigableMap();
            String bitmapValue = jedisPool.get(bitmapKey);
            LOG.info("Get bitmapValue: {}", bitmapValue);
            if (StringUtils.isNotBlank(bitmapValue)) {
                bitmap = RoaringBitmapUtil.parseBitmap(bitmapValue);
            }
            if (!bitmap.contains(uid)) {
                bitmap.addLong(uid);
                String newValue = RoaringBitmapUtil.parseString(bitmap);
                jedisPool.set(bitmapKey, newValue);
                LOG.info("Set bitmapValue: {}", newValue);
            }

            long bitmapCount = bitmap.getLongCardinality();
            long bitCount = jedisPool.bitcount(bitKey);
            long pfCount = jedisPool.pfcount(hlKey);

//            if ((bitCount <= 1000 && bitCount % 10 == 0) ||
//                    (bitCount > 1000 && bitCount <= 10000 && bitCount % 100 == 0) ||
//                    (bitCount > 10000 && bitCount <= 100000 && bitCount % 1000 == 0) ||
//                    (bitCount > 100000 && bitCount <= 1000000 && bitCount % 10000 == 0) ||
//                    (bitCount > 1000000 && bitCount <= 10000000 && bitCount % 10000 == 0) ||
//                    (bitCount > 10000000 && bitCount <= 100000000 && bitCount % 100000 == 0)) {
//                LOG.info("uid: {}, bitCount: {}, pfCount: {}, bitmapCount: {}, diff: {}, ratio: {}",
//                        uid, bitCount, pfCount, bitmapCount,
//                        (bitCount - pfCount), (bitCount - pfCount)*1.0/bitCount*100
//                );
//            }
            LOG.info("uid: {}, bitCount: {}, pfCount: {}, bitmapCount: {}, diff: {}, ratio: {}",
                    uid, bitCount, pfCount, bitmapCount,
                    (bitCount - pfCount), (bitCount - pfCount)*1.0/bitCount*100
            );
        }

        @Override
        public void close() throws IOException {
            if (jedisPool != null) {
                jedisPool.close();
            }
        }
    }
}

