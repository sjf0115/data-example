package com.flink.example.test;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * TimerProcessFunction 单元测试
 * Created by wy on 2020/11/9.
 */
public class TimerProcessFunctionUnitTest {

    private OneInputStreamOperatorTestHarness<String, String> testHarness;
    private TimerProcessFunction processFunction;

    @Before
    public void setupTestHarness() throws Exception {
        processFunction = new TimerProcessFunction();

        // KeyedOneInputStreamOperatorTestHarness 需要三个参数：算子对象、键 Selector、键类型
        testHarness = new KeyedOneInputStreamOperatorTestHarness<>(
                new KeyedProcessOperator<>(processFunction),
                x -> "1",
                Types.STRING
        );
        // Function time is initialized to 0
        testHarness.open();
    }

    @Test
    public void testProcessElement() throws Exception{

        testHarness.processElement("world", 10);
        Assert.assertEquals(
                Lists.newArrayList(
                        new StreamRecord<>("hello world", 10)
                ),
                testHarness.extractOutputStreamRecords()
        );
    }

    @Test
    public void testOnTimer() throws Exception {
        // test first record
        testHarness.processElement("world", 10);
        Assert.assertEquals(1, testHarness.numProcessingTimeTimers());

        // Function time 设置为 50
        testHarness.setProcessingTime(50);
        Assert.assertEquals(
                Lists.newArrayList(
                        new StreamRecord<>("hello world", 10),
                        new StreamRecord<>("Timer triggered at timestamp 50")
                ),
                testHarness.extractOutputStreamRecords()
        );
    }
}
