package com.flink.example.stream.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 功能：
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/3/5 上午10:58
 */
public class SocketSourceExample {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 原生 SocketTextStreamFunction
        // DataStream<String> source = env.socketTextStream("localhost", 9100, "\n");

        // 自定义 SocketSourceFunction
        SocketSourceFunction socketSourceFunction = new SocketSourceFunction("localhost", 9100, "&", 3);
        DataStreamSource<String> source = env.addSource(socketSourceFunction, "SocketSource");
        // 输出
        source.print();
        env.execute("SocketSourceExample");
    }
}
