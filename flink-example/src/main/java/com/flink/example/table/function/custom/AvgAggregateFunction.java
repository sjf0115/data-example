package com.flink.example.table.function.custom;

import org.apache.flink.table.functions.AggregateFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 功能：自定义聚合函数 Avg
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/5/19 上午8:54
 */
public class AvgAggregateFunction extends AggregateFunction<Double, AvgAggregateFunction.AvgAccumulator>{

    private static final Logger LOG = LoggerFactory.getLogger(AvgAggregateFunction.class);

    // 聚合中间结果数据结构
    public static class AvgAccumulator {
        public long sum = 0;
        public int count = 0;
    }

    // 创建 Accumulator 数据结构
    @Override
    public AvgAccumulator createAccumulator() {
        LOG.info("[INFO] createAccumulator ...................");
        return new AvgAccumulator();
    }

    // 返回最终结果 平均值
    @Override
    public Double getValue(AvgAccumulator acc) {
        LOG.info("[INFO] getValue ...................");
        if (acc.count == 0) {
            return null;
        } else {
            return acc.sum * 1.0 / acc.count;
        }
    }

    // 接收输入元素并累加到 Accumulator 数据结构
    public void accumulate(AvgAccumulator acc, Long value) {
        LOG.info("[INFO] accumulate ...................");
        acc.sum += value;
        acc.count ++;
    }

    // 非必须:回撤
    public void retract(AvgAccumulator acc, Long value) {
        LOG.info("[INFO] retract ...................");
        acc.sum -= value;
        acc.count --;
    }

    // 非必须:合并 Accumulator 数据结构
    public void merge(AvgAccumulator acc, Iterable<AvgAccumulator> iterable) {
        LOG.info("[INFO] merge ...................");
        for (AvgAccumulator a : iterable) {
            acc.count += a.count;
            acc.sum += a.sum;
        }
    }

    // 非必须:重置 Accumulator 数据结构
    public void resetAccumulator(AvgAccumulator acc) {
        LOG.info("[INFO] resetAccumulator ...................");
        acc.count = 0;
        acc.sum = 0L;
    }
}
