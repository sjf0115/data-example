package com.flink.example.table.function.custom;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.functions.TableAggregateFunction;

import java.util.Objects;

/**
 * 功能：Top2 表聚合函数 带回撤输出
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/5/20 下午10:41
 */
public class Top2RetractTableAggregateFunction extends TableAggregateFunction<Tuple2<Long, Integer>, Top2RetractTableAggregateFunction.Top2Accumulator> {
    // Top2 聚合中间结果数据结构
    public static class Top2Accumulator {
        public long beforeFirst = 0;
        public long beforeSecond = 0;
        public long afterFirst = 0;
        public long afterSecond = 0;
    }

    @Override
    public Top2Accumulator createAccumulator() {
        Top2Accumulator acc = new Top2Accumulator();
        acc.beforeFirst = Integer.MIN_VALUE;
        acc.beforeSecond = Integer.MIN_VALUE;
        acc.afterFirst = Integer.MIN_VALUE;
        acc.afterSecond = Integer.MIN_VALUE;
        return acc;
    }

    public void accumulate(Top2Accumulator acc, Long value) {
        if (value > acc.afterFirst) {
            acc.afterSecond = acc.afterFirst;
            acc.afterFirst = value;
        } else if (value > acc.afterSecond) {
            acc.afterSecond = value;
        }
    }

    public void emitUpdateWithRetract(Top2Accumulator acc, RetractableCollector<Tuple2<Long, Integer>> out) {
        if (!Objects.equals(acc.afterFirst, acc.beforeFirst)) {
            if (acc.beforeFirst != Integer.MIN_VALUE) {
                out.retract(Tuple2.of(acc.beforeFirst, 1));
            }
            out.collect(Tuple2.of(acc.afterFirst, 1));
            acc.beforeFirst = acc.afterFirst;
        }
        if (!Objects.equals(acc.afterSecond, acc.beforeSecond)) {
            if (acc.beforeSecond != Integer.MIN_VALUE) {
                out.retract(Tuple2.of(acc.beforeSecond, 2));
            }
            out.collect(Tuple2.of(acc.afterSecond, 2));
            acc.beforeSecond = acc.afterSecond;
        }
    }

    public void merge(Top2Accumulator acc, Iterable<Top2Accumulator> iterable) {
        for (Top2Accumulator otherAcc : iterable) {
            // 复用 accumulate 方法
            accumulate(acc, otherAcc.afterFirst);
            accumulate(acc, otherAcc.afterSecond);
        }
    }
}
