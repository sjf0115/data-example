package com.flink.example.stream.source.socket;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 功能：SocketSourceExample
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/3/5 上午10:58
 */
public class SocketSourceExample {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 原生 SocketTextStreamFunction
        DataStream<String> source = env.socketTextStream("localhost", 9000, "\n");
        source.print("1");

        // 自定义 SocketSourceFunction
        SocketSourceFunction socketSourceFunction = new SocketSourceFunction("localhost", 9100, "\n");
        DataStreamSource<String> source1 = env.addSource(socketSourceFunction, "SocketSource");
        source1.print("2");

        // 执行
        env.execute("SocketSourceExample");
    }
}
