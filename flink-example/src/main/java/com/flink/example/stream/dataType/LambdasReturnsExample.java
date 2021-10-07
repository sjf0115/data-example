package com.flink.example.stream.dataType;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 功能：Lambdas Returns 示例
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2021/9/4 下午8:34
 */
public class LambdasReturnsExample {
    private static final Logger LOG = LoggerFactory.getLogger(LambdasReturnsExample.class);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> source = env.socketTextStream("localhost", 9100, "\n");

        DataStream<Tuple2<String, String>> result = source
                .map(new MapFunction<String, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> map(String str) throws Exception {
                        String[] array = str.split(",");
                        String key = array[0];
                        String value = array[1];
                        LOG.info("Key: {}, Value: {}", key, value);
                        return new Tuple2<>(key, value);
                    }
                });

        //.returns(Types.TUPLE(Types.STRING, Types.LONG))

        result.print();
        env.execute("LambdasReturnsExample");
    }
}
