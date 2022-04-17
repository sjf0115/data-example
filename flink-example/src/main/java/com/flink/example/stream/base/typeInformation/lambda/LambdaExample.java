package com.flink.example.stream.base.typeInformation.lambda;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.util.Collector;

/**
 * 功能：
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2021/10/7 下午9:42
 */
public class LambdaExample {

    // Lambda 表达式
    private static void map1(ExecutionEnvironment env) throws Exception {
        env.fromElements(1, 2, 3)
                .map(i -> i*i)
                .print();
    }

    // Lambda 表达式 与泛型
    private static void map2(ExecutionEnvironment env) throws Exception {
        env.fromElements(1, 2, 3)
                .map(i -> Tuple2.of(i, i*i))
                .print();
    }

    // Lambda 表达式,使用 returns 显示指明类型信息
    private static void map3(ExecutionEnvironment env) throws Exception {
        env.fromElements(1, 2, 3)
                .map(i -> Tuple2.of(i, i*i))
                .returns(Types.TUPLE(Types.INT, Types.INT))
                .print();
    }

    // 匿名内部类代替 Lambda 表达式
    private static void map4(ExecutionEnvironment env) throws Exception {
        env.fromElements(1, 2, 3)
                .map(new MapFunction<Integer, Tuple2<Integer, Integer>>() {
                    @Override
                    public Tuple2<Integer, Integer> map(Integer i) throws Exception {
                        return new Tuple2<Integer, Integer>(i, i * i);
                    }
                })
                .print();
    }

    public static class MyMapFunction implements MapFunction<Integer, Tuple2<Integer, Integer>> {
        @Override
        public Tuple2<Integer, Integer> map(Integer i) {
            return Tuple2.of(i, i*i);
        }
    }

    // 自定义类代替 Lambda 表达式
    private static void map5(ExecutionEnvironment env) throws Exception {
        env.fromElements(1, 2, 3)
                .map(new MyMapFunction())
                .print();
    }

    private static void flapMap1(ExecutionEnvironment env) throws Exception {
        env.fromElements("1,2,3", "4,5")
                .flatMap((String input, Collector<String> out) -> {
                    String[] params = input.split(",");
                    for(String value : params) {
                        out.collect(value);
                    }
                })
                .print();
    }

    // 匿名内部类
    private static void flapMap2(ExecutionEnvironment env) throws Exception {
        env.fromElements("1,2,3", "4,5")
                .flatMap(new FlatMapFunction<String, String>() {
                    @Override
                    public void flatMap(String input, Collector<String> out) throws Exception {
                        String[] params = input.split(",");
                        for(String value : params) {
                            out.collect(value);
                        }
                    }
                })
                .print();
    }

    public static class MyFlatMapFunction implements FlatMapFunction<String, String> {
        @Override
        public void flatMap(String input, Collector<String> out) throws Exception {
            String[] params = input.split(",");
            for(String value : params) {
                out.collect(value);
            }
        }
    }

    // 自定义类代替 Lambda 表达式
    private static void flapMap3(ExecutionEnvironment env) throws Exception {
        env.fromElements("1,2,3", "4,5")
                .flatMap(new MyFlatMapFunction())
                .print();
    }

    public static class ResultTypeFlatMapFunction implements ResultTypeQueryable<String>, FlatMapFunction<String, String> {
        @Override
        public void flatMap(String input, Collector<String> out) throws Exception {
            String[] params = input.split(",");
            for(String value : params) {
                out.collect(value);
            }
        }

        @Override
        public TypeInformation<String> getProducedType() {
            return TypeInformation.of(String.class);
        }
    }

    // ResultTypeQueryable 接口
    private static void flapMap4(ExecutionEnvironment env) throws Exception {
        env.fromElements("1,2,3", "4,5")
                .flatMap(new ResultTypeFlatMapFunction())
                .print();
    }

    // Lambda 表达式,使用 returns 显示指明类型信息
    private static void flapMap5(ExecutionEnvironment env) throws Exception {
        env.fromElements("1,2,3", "4,5")
                .flatMap((String input, Collector<String> out) -> {
                    String[] params = input.split(",");
                    for(String value : params) {
                        out.collect(value);
                    }
                })
                .returns(String.class)
                .print();
    }

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        map2(env);
    }
}
