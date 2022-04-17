package com.flink.example.stream.base.typeInformation.hints;

import com.common.example.bean.WordCount;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

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

        // 示例1
        DataStream<Tuple2<String, Integer>> result1 = env.fromElements("a", "b", "a")
                .map(value -> Tuple2.of(value, 1))
                .returns(Types.TUPLE(Types.STRING, Types.INT));
        result1.print("R1");

        // 示例2
        DataStream<WordCount> result2 = env.fromElements("a b a")
                .flatMap((String value, Collector<WordCount> out) -> {
                    for(String word : value.split("\\s")) {
                        out.collect(new WordCount(word, 1));
                    }
                })
                .returns(TypeInformation.of(WordCount.class));
        result2.print("R2");

        env.execute("ReturnsExample");
    }
}
