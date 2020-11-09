package com.flink.example.test;

import com.google.common.collect.Lists;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.junit.Assert;
import org.junit.Test;

/**
 * ProcessFunction 单元测试
 * Created by wy on 2020/11/9.
 */
public class ProcessFunctionUnitTest {
    @Test
    public void testPassThrough() throws Exception {

        MyProcessFunction processFunction = new MyProcessFunction();

        OneInputStreamOperatorTestHarness<Integer, Integer> testHarness = ProcessFunctionTestHarnesses
                .forProcessFunction(processFunction);

        testHarness.processElement(1, 10);
        Assert.assertEquals(
                Lists.newArrayList(
                        new StreamRecord<>(1, 10)
                ),
                testHarness.extractOutputStreamRecords()
        );
    }
}
