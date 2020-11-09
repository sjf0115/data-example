package com.flink.example.test;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * 无状态算子单元测试 FlatMap
 * Created by wy on 2020/11/8.
 */
public class MyStatelessFlatMap implements FlatMapFunction<String, String> {
    @Override
    public void flatMap(String s, Collector<String> collector) throws Exception {
        String out = "hello " + s;
        collector.collect(out);
    }
}
