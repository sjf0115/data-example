package com.flink.example.test;

import org.apache.flink.api.common.functions.MapFunction;

/**
 * 无状态算子单元测试 Map
 * Created by wy on 2020/11/8.
 */
public class MyStatelessMap implements MapFunction<String, String> {
    @Override
    public String map(String s) throws Exception {
        String out = "hello " + s;
        return out;
    }
}
