package com.flink.example.stream.dataType;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 功能：Returns 示例
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2021/10/5 下午6:08
 */
public class ReturnsExample {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> source = env.socketTextStream("localhost", 9100, "\n");

        DataStream<Tuple2<String, Integer>> result = source
                .map(value -> Tuple2.of(value.split(",")[0], Integer.parseInt(value.split(",")[1])))
                .returns(Types.TUPLE(Types.STRING, Types.INT));
        result.print();

        env.execute("ReturnsExample");
    }
}
