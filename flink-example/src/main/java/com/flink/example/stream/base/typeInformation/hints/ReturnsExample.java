package com.flink.example.stream.base.typeInformation.hints;

import com.common.example.bean.WordCount;
import org.apache.flink.api.common.typeinfo.TypeHint;
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

        // 示例1 非泛型类型 直接传入 Class
        DataStream<WordCount> result1 = env.fromElements("a b a")
                .flatMap((String value, Collector<WordCount> out) -> {
                    for(String word : value.split("\\s")) {
                        out.collect(new WordCount(word, 1));
                    }
                })
                .returns(WordCount.class);
        result1.print("R1");

        // 示例2 泛型类型 优先推荐借助 TypeHint
        DataStream<Tuple2<String, Integer>> result2 = env.fromElements("a", "b", "a")
                .map(value -> Tuple2.of(value, 1))
                .returns(new TypeHint<Tuple2<String, Integer>>() {});
        result2.print("R2");

        // 示例3 TypeInformation.of + TypeHint
        DataStream<Tuple2<String, Integer>> result3 = env.fromElements("a", "b", "a")
                .map(value -> Tuple2.of(value, 1))
                .returns(TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {}));
        result3.print("R3");

        // 示例4 Types 快捷方式
        DataStream<Tuple2<String, Integer>> result4 = env.fromElements("a", "b", "a")
                .map(value -> Tuple2.of(value, 1))
                .returns(Types.TUPLE(Types.STRING, Types.INT));
        result4.print("R4");

        env.execute("ReturnsExample");
    }
}
