package com.flink.example.table.function.custom;

import org.apache.flink.table.functions.ScalarFunction;

/**
 * 功能：Long 转 Timestamp
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/5/17 下午10:53
 */
public class LongToTimestampScalarFunction extends ScalarFunction {
    public Long eval (Long timestamp) {
        return timestamp % 1000;
    }
}
