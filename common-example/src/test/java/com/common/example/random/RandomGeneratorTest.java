package com.common.example.random;

import org.junit.Test;

/**
 * 功能：RandomGenerator 测试
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2025/1/3 22:17
 */
public class RandomGeneratorTest {
    @Test
    public void testLongGenerator() {
        RandomGenerator<Long> generator = RandomGenerator.longGenerator(5, 10);
        System.out.println(generator.next());
    }

    @Test
    public void testStringGenerator() {
        RandomGenerator<String> generator = RandomGenerator.stringGenerator(6);
        System.out.println(generator.next());
    }
}
