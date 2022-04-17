package com.flink.example.stream.base.typeInformation.hints;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 功能：TypeHint 示例
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2021/10/4 下午12:07
 */
public class TypeHintExample {
    public static void main(String[] args) throws Exception {
        // 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 示例1 TypeInformation.of + TypeHint
        DataStream<Tuple2<String, Integer>> result1 = env.fromElements("a", "b", "a")
                .map(value -> Tuple2.of(value, 1))
                .returns(TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {}));
        result1.print("R1");

        // 示例2 只借助 TypeHint
        DataStream<Tuple2<String, Integer>> result2 = env.fromElements("a", "b", "a")
                .map(value -> Tuple2.of(value, 1))
                .returns(new TypeHint<Tuple2<String, Integer>>() {}.getTypeInfo());
        result2.print("R2");

        env.execute();
    }
}
//R2:1> (a,1)
//R2:3> (a,1)
//R2:4> (b,1)
//R1:1> (a,1)
//R1:3> (a,1)
//R1:4> (b,1)