package com.flink.example.test;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.util.ListCollector;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * 无状态算子单元测试
 * Created by wy on 2020/11/8.
 */
public class StatelessUnitTest {
    @Test
    public void MyStatelessMap() throws Exception {
        MyStatelessMap statelessMap = new MyStatelessMap();
        String out = statelessMap.map("world");
        Assert.assertEquals("hello world", out);
    }

    @Test
    public void MyStatelessFlatMap() throws Exception {
        MyStatelessFlatMap statelessFlatMap = new MyStatelessFlatMap();
        List<String> out = new ArrayList<>();
        ListCollector<String> listCollector = new ListCollector<>(out);
        statelessFlatMap.flatMap("world", listCollector);
        Assert.assertEquals(Lists.newArrayList("hello world"), out);
    }
}
