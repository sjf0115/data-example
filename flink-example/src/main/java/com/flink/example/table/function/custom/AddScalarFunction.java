package com.flink.example.table.function.custom;

import org.apache.flink.table.functions.ScalarFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 功能：Add 标量函数实现
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/5/17 下午10:01
 */
public class AddScalarFunction extends ScalarFunction {

    private static final Logger LOG = LoggerFactory.getLogger(AddScalarFunction.class);

    public Integer eval (Integer a, Integer b) {
        LOG.info("执行 Integer eval (Integer a, Integer b)");
        return a + b;
    }

    public Integer eval(String a, String b) {
        LOG.info("执行 Integer eval(String a, String b)");
        return Integer.valueOf(a) + Integer.valueOf(b);
    }

    public Long eval(Long... values) {
        LOG.info("执行 Long eval(Long... values)");
        Long result = 0L;
        for (Long value : values) {
            result += value;
        }
        return result;
    }
}
