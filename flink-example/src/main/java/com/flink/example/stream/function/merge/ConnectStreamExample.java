package com.flink.example.stream.function.merge;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Connect 流合并 示例
 * Created by wy on 2021/3/24.
 */
public class ConnectStreamExample {
    private static final Logger LOG = LoggerFactory.getLogger(ConnectStreamExample.class);

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // A输入流
        DataStream<String> aStream = env.socketTextStream("localhost", 9100, "\n");
        // B输入流
        DataStream<String> bStream = env.socketTextStream("localhost", 9101, "\n");

        KeySelector<Tuple2<String, String>, String> keySelector = new KeySelector<Tuple2<String, String>, String>() {
            @Override
            public String getKey(Tuple2<String, String> tuple2) throws Exception {
                return tuple2.f0;
            }
        };

        // 合并流
        ConnectedStreams<String, String> connectStream = aStream.connect(bStream);
        DataStream<Tuple2<String, String>> result = connectStream
                .map(new CoMapFunction<String, String, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> map1(String s) throws Exception {
                        LOG.info("[A流] Value: {}", s);
                        String[] split = s.split(",");
                        return Tuple2.of(split[0], split[1]);
                    }

                    @Override
                    public Tuple2<String, String> map2(String s) throws Exception {
                        LOG.info("[B流] Value: {}", s);
                        String[] split = s.split(",");
                        return Tuple2.of(split[0], split[1]);
                    }
                })
                .keyBy(keySelector);

        result.print();

        env.execute("ConnectStreamExample");
    }
}
