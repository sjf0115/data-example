package com.flink.example.stream.tuning.localAgg;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.util.Map;

/**
 * 功能：批次聚合函数 实现一个批次的聚合 按照 Key 聚合
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2022/10/15 下午11:04
 */
public abstract class BundleFunction<K, V, IN, OUT> implements Function {
    // 将一个数据记录元素添加到批次中并返回按Key聚合后的批次值
    public abstract V addElement(@Nullable V value, IN element) throws Exception;
    // 积攒一个批次时输出批次 可以转换为0个,1个或者多个输出数据记录
    public abstract void finishBundle(Map<K, V> bundle, Collector<OUT> out) throws Exception;
}
