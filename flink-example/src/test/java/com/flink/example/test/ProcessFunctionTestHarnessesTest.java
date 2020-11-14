package com.flink.example.test;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.apache.flink.streaming.util.TwoInputStreamOperatorTestHarness;
import org.apache.flink.util.Collector;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

/**
 * ProcessFunction 单元测试示例
 * Created by wy on 2020/11/9.
 */
public class ProcessFunctionTestHarnessesTest {
    @Test
    public void testHarnessForProcessFunction() throws Exception {
        ProcessFunction<Integer, Integer> function = new ProcessFunction<Integer, Integer> () {

            @Override
            public void processElement(
                    Integer value, Context ctx, Collector<Integer> out) throws Exception {
                out.collect(value);
            }
        };
        OneInputStreamOperatorTestHarness<Integer, Integer> harness = ProcessFunctionTestHarnesses
                .forProcessFunction(function);

        harness.processElement(1, 10);

        Assert.assertEquals(harness.extractOutputValues(), Collections.singletonList(1));
    }

    @Test
    public void testHarnessForKeyedProcessFunction() throws Exception {
        KeyedProcessFunction<Integer, Integer, Integer> function = new KeyedProcessFunction<Integer, Integer, Integer>() {
            @Override
            public void processElement(Integer value, Context ctx, Collector<Integer> out) throws Exception {
                out.collect(value);
            }
        };
        OneInputStreamOperatorTestHarness<Integer, Integer> harness = ProcessFunctionTestHarnesses
                .forKeyedProcessFunction(function, x -> x, BasicTypeInfo.INT_TYPE_INFO);

        harness.processElement(1, 10);

        Assert.assertEquals(harness.extractOutputValues(), Collections.singletonList(1));
    }

    @Test
    public void testHarnessForCoProcessFunction() throws Exception {
        CoProcessFunction<Integer, String, Integer> function = new CoProcessFunction<Integer, String, Integer>() {

            @Override
            public void processElement1(Integer value, Context ctx, Collector<Integer> out) throws Exception {
                out.collect(value);
            }

            @Override
            public void processElement2(String value, Context ctx, Collector<Integer> out) throws Exception {
                out.collect(Integer.parseInt(value));
            }
        };
        TwoInputStreamOperatorTestHarness<Integer, String, Integer> harness = ProcessFunctionTestHarnesses
                .forCoProcessFunction(function);

        harness.processElement2("0", 1);
        harness.processElement1(1, 10);

        Assert.assertEquals(harness.extractOutputValues(), Arrays.asList(0, 1));
    }

    @Test
    public void testHarnessForKeyedCoProcessFunction() throws Exception {
        KeyedCoProcessFunction<Integer, Integer, Integer, Integer> function = new KeyedCoProcessFunction<Integer, Integer, Integer, Integer>() {

            @Override
            public void processElement1(Integer value, Context ctx, Collector<Integer> out) throws Exception {
                out.collect(value);
            }

            @Override
            public void processElement2(Integer value, Context ctx, Collector<Integer> out) throws Exception {
                out.collect(value);
            }
        };

        KeyedTwoInputStreamOperatorTestHarness<Integer, Integer, Integer, Integer> harness = ProcessFunctionTestHarnesses
                .forKeyedCoProcessFunction(function, x -> x, x -> x, TypeInformation.of(Integer.class));

        harness.processElement1(0, 1);
        harness.processElement2(1, 10);

        Assert.assertEquals(harness.extractOutputValues(), Arrays.asList(0, 1));
    }
}
