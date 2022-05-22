package com.flink.example.table.function.custom;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * 功能：Top2 表聚合函数 带撤回输出
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/5/20 下午10:41
 */
public class Top2RetractTableAggregateFunction extends TableAggregateFunction<Tuple2<Long, Integer>, Top2RetractTableAggregateFunction.Top2RetractAccumulator> {
    private static final Logger LOG = LoggerFactory.getLogger(Top2RetractTableAggregateFunction.class);
    // Top2 聚合中间结果数据结构
    public static class Top2RetractAccumulator {
        public long beforeFirst = 0;
        public long beforeSecond = 0;
        public long afterFirst = 0;
        public long afterSecond = 0;
    }

    // 创建 Top2Accumulator 累加器并做初始化
    @Override
    public Top2RetractAccumulator createAccumulator() {
        LOG.info("[INFO] createAccumulator ...........................");
        Top2RetractAccumulator acc = new Top2RetractAccumulator();
        acc.beforeFirst = Integer.MIN_VALUE;
        acc.beforeSecond = Integer.MIN_VALUE;
        acc.afterFirst = Integer.MIN_VALUE;
        acc.afterSecond = Integer.MIN_VALUE;
        return acc;
    }

    // 接收输入元素并累加到 Accumulator 数据结构
    public void accumulate(Top2RetractAccumulator acc, Long value) {
        LOG.info("[INFO] accumulate ...........................");
        if (value > acc.afterFirst) {
            acc.afterSecond = acc.afterFirst;
            acc.afterFirst = value;
        } else if (value > acc.afterSecond) {
            acc.afterSecond = value;
        }
    }

    // 撤回
    public void retract(Top2RetractAccumulator acc, Integer value){
        LOG.info("[INFO] retract ...........................");
        if (value == acc.afterFirst) {
            acc.beforeFirst = acc.afterFirst;
            acc.beforeSecond = acc.afterSecond;
            acc.afterFirst = acc.afterSecond;
            acc.afterSecond = Integer.MIN_VALUE;
        } else if (value == acc.afterSecond) {
            acc.beforeSecond = acc.afterSecond;
            acc.afterSecond = Integer.MIN_VALUE;
        }
    }

    // 非撤回的输出
//    public void emitValue(Top2RetractAccumulator acc, Collector<Tuple2<Long, Integer>> out) {
//        LOG.info("[INFO] emitValue ...........................");
//        if (acc.afterFirst != Integer.MIN_VALUE) {
//            out.collect(Tuple2.of(acc.afterFirst, 1));
//        }
//        if (acc.afterSecond != Integer.MIN_VALUE) {
//            out.collect(Tuple2.of(acc.afterSecond, 2));
//        }
//    }

    // 带撤回的输出
    public void emitUpdateWithRetract(Top2RetractAccumulator acc, RetractableCollector<Tuple2<Long, Integer>> out) {
        LOG.info("[INFO] emitUpdateWithRetract ...........................");
        if (!Objects.equals(acc.afterFirst, acc.beforeFirst)) {
            // 撤回旧记录
            if (acc.beforeFirst != Integer.MIN_VALUE) {
                out.retract(Tuple2.of(acc.beforeFirst, 1));
            }
            // 输出新记录
            out.collect(Tuple2.of(acc.afterFirst, 1));
            acc.beforeFirst = acc.afterFirst;
        }
        if (!Objects.equals(acc.afterSecond, acc.beforeSecond)) {
            // 撤回旧记录
            if (acc.beforeSecond != Integer.MIN_VALUE) {
                out.retract(Tuple2.of(acc.beforeSecond, 2));
            }
            // 输出新记录
            out.collect(Tuple2.of(acc.afterSecond, 2));
            acc.beforeSecond = acc.afterSecond;
        }
    }
}
