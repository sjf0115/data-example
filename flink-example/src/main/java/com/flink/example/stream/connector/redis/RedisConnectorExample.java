package com.flink.example.stream.connector.redis;

import com.common.example.bean.LoginUser;
import com.flink.example.stream.source.simple.UserPressureMockSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 功能：RedisConnectorExample
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2022/10/27 下午11:38
 */
public class RedisConnectorExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<LoginUser> source = env.addSource(new UserPressureMockSource());

        SingleOutputStreamOperator<Tuple2<Long, Integer>> result = source
                .map(new MapFunction<LoginUser, Tuple2<Long, Integer>>() {
                    @Override
                    public Tuple2<Long, Integer> map(LoginUser user) throws Exception {
                        return Tuple2.of(user.getUid(), 1);
                    }
                })
                .keyBy(new KeySelector<Tuple2<Long, Integer>, Long>() {
                    @Override
                    public Long getKey(Tuple2<Long, Integer> user) throws Exception {
                        return user.f0;
                    }
                }).reduce(new ReduceFunction<Tuple2<Long, Integer>>() {
                    @Override
                    public Tuple2<Long, Integer> reduce(Tuple2<Long, Integer> value1, Tuple2<Long, Integer> value2) throws Exception {
                        return new Tuple2<>(value1.f0, value1.f1 + value2.f1);
                    }
                });

        FlinkJedisPoolConfig config = new FlinkJedisPoolConfig.Builder()
                .setHost("localhost")
                .setPort(6379)
                .build();

        result.addSink(new RedisSink(config));

        env.execute();
    }
}
