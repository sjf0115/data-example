package com.flink.example.test;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * ProcessFunction 单元测试
 * Created by wy on 2020/11/9.
 */
public class MyProcessFunction extends ProcessFunction<Integer, Integer> {
    @Override
    public void processElement(Integer integer, Context context, Collector<Integer> collector) throws Exception {
        collector.collect(integer);
    }
}
