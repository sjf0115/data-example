package com.flink.example.stream.function.merge;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 功能：使用 Connect 连接流
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2022/9/13 上午9:33
 */
public class ConnectStreamExample {
    private static final Logger LOG = LoggerFactory.getLogger(ConnectStreamExample.class);

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // A输入流
        DataStream<String> aStream = env.fromElements("1", "3", "5");
        // B输入流
        DataStream<Integer> bStream = env.fromElements(2, 4, 6);

        // 使用 Connect 连接两个流
        ConnectedStreams<String, Integer> connectStream = aStream.connect(bStream);
        // 处理连接流
        SingleOutputStreamOperator<Long> mapStream = connectStream
                .map(new CoMapFunction<String, Integer, Long>() {
                    @Override
                    public Long map1(String value) throws Exception {
                        LOG.info("[A流] Value: {}", value);
                        return Long.parseLong(value);
                    }

                    @Override
                    public Long map2(Integer value) throws Exception {
                        LOG.info("[B流] Value: {}", value);
                        return value.longValue();
                    }
                });
        // 输出
        mapStream.print();

        env.execute("ConnectStreamExample");
    }
}
