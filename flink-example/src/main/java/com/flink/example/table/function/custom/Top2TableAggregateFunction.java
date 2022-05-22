package com.flink.example.table.function.custom;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.util.Collector;

/**
 * 功能：Top2 表聚合函数
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/5/20 下午10:41
 */
public class Top2TableAggregateFunction extends TableAggregateFunction<Tuple2<Long, Integer>, Top2TableAggregateFunction.Top2Accumulator> {
    // Top2 聚合中间结果数据结构
    public static class Top2Accumulator {
        public long first = 0;
        public long second = 0;
    }

    // 创建 Top2Accumulator 累加器并做初始化
    @Override
    public Top2Accumulator createAccumulator() {
        Top2Accumulator acc = new Top2Accumulator();
        acc.first = Integer.MIN_VALUE;
        acc.second = Integer.MIN_VALUE;
        return acc;
    }

    // 接收输入元素并累加到 Accumulator 数据结构
    public void accumulate(Top2Accumulator acc, Long value) {
        if (value > acc.first) {
            acc.second = acc.first;
            acc.first = value;
        } else if (value > acc.second) {
            acc.second = value;
        }
    }

    // 输出元素
    public void emitValue(Top2Accumulator acc, Collector<Tuple2<Long, Integer>> out) {
        if (acc.first != Integer.MIN_VALUE) {
            out.collect(Tuple2.of(acc.first, 1));
        }
        if (acc.second != Integer.MIN_VALUE) {
            out.collect(Tuple2.of(acc.second, 2));
        }
    }

    // 合并
    public void merge(Top2Accumulator acc, Iterable<Top2Accumulator> iterable) {
        for (Top2Accumulator otherAcc : iterable) {
            // 复用 accumulate 方法
            accumulate(acc, otherAcc.first);
            accumulate(acc, otherAcc.second);
        }
    }
}
