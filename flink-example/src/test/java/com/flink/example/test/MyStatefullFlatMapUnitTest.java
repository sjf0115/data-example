package com.flink.example.test;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.StreamFlatMap;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * 有状态算子单元测试
 * Created by wy on 2020/11/9.
 */
public class MyStatefullFlatMapUnitTest {

    private KeyedOneInputStreamOperatorTestHarness<String, String, Long>  testHarness;
    private MyStatefulFlatMap statefulFlatMap;

    @Before
    public void setupTestHarness() throws Exception {
        statefulFlatMap = new MyStatefulFlatMap();
        // KeyedOneInputStreamOperatorTestHarness 需要三个参数：算子对象、键 Selector、键类型
        testHarness = new KeyedOneInputStreamOperatorTestHarness<>(
                new StreamFlatMap<>(statefulFlatMap),
                x -> "1",
                Types.STRING
        );
        testHarness.open();
    }

    @Test
    public void MyStatefulFlatMap() throws Exception{
        // test first record
        testHarness.processElement("a", 10);
        //
        Assert.assertEquals(
                Lists.newArrayList(new StreamRecord<>(1L, 10)),
                this.testHarness.extractOutputStreamRecords()
        );

        // test second record
        testHarness.processElement("b", 20);
        Assert.assertEquals(
                Lists.newArrayList(
                        new StreamRecord<>(1L, 10),
                        new StreamRecord<>(2L, 20)
                ),
                testHarness.extractOutputStreamRecords()
        );

        // test other record
        testHarness.processElement("c", 30);
        testHarness.processElement("d", 40);
        testHarness.processElement("e", 50);
        Assert.assertEquals(
                Lists.newArrayList(
                        new StreamRecord<>(1L, 10),
                        new StreamRecord<>(2L, 20),
                        new StreamRecord<>(3L, 30),
                        new StreamRecord<>(4L, 40),
                        new StreamRecord<>(5L, 50)
                ),
                testHarness.extractOutputStreamRecords()
        );
    }
}
