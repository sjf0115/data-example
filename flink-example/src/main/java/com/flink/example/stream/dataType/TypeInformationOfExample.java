package com.flink.example.stream.dataType;

import com.common.example.bean.WordCount;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 功能：TypeInformation Of 示例
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/4/17 下午9:44
 */
public class TypeInformationOfExample {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 示例1 非泛型类型 直接传入 Class 对象
        DataStream<WordCount> result1 = env.fromElements("a b a")
                .flatMap((String value, Collector<WordCount> out) -> {
                    for(String word : value.split("\\s")) {
                        out.collect(new WordCount(word, 1));
                    }
                })
                .returns(TypeInformation.of(WordCount.class));
        result1.print("R1");

        // 示例2 泛型类型 需要借助 TypeHint
        DataStream<Tuple2<String, Integer>> result2 = env.fromElements("a", "b", "a")
                .map(value -> Tuple2.of(value, 1))
                .returns(TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {}));
        result2.print("R2");

        env.execute("TypeInformationOfExample");
    }
}
//R2:1> (a,1)
//R1:3> WordCount{word='a', frequency=1}
//R2:3> (a,1)
//R1:3> WordCount{word='b', frequency=1}
//R1:3> WordCount{word='a', frequency=1}
//R2:4> (b,1)