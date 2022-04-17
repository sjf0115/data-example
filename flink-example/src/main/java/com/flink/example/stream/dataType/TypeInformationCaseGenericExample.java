package com.flink.example.stream.dataType;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
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
        env.fromElements(1, 2, 3)
                .map(i -> Tuple2.of(i, i*i))
                // 如果不指定 returns 返回的 TypeInformation 会抛出异常
                //.returns(Types.TUPLE(Types.INT, Types.INT))
                .returns(TypeInformation.of(new TypeHint<Tuple2<Integer, Integer>>(){}))
                .print();
        env.execute();
    }
}
//1> (1,1)
//2> (2,4)
//3> (3,9)
