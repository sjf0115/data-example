package com.flink.example.stream.dataType;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 功能：TypeInformation 使用场景 泛型
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/4/17 下午6:02
 */
public class TypeInformationCaseGenericExample {
    public static void main(String[] args) throws Exception {
        // 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Tuple2<String, Integer>> result = env.fromElements("a", "b", "a")
                .map(value -> Tuple2.of(value, 1))
                .returns(TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {}));
        result.print();

        env.execute();
    }
}
//3> (a,1)
//4> (b,1)
//1> (a,1)
