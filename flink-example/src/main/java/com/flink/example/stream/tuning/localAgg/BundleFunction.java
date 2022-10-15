package com.flink.example.stream.tuning.localAgg;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.util.Map;

/**
 * 功能：批次处理函数
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2022/10/15 下午11:04
 */
public abstract class BundleFunction<K, V, IN, OUT> implements Function {
    // 添加新输入到指定的批次值中并返回一个新的批次值
    public abstract V addElement(@Nullable V value, IN element) throws Exception;
    // 当完成一个批次的时候调用 将一个批次转换为0个,1个或者多个输出数据记录
    public abstract void finishBundle(Map<K, V> bundle, Collector<OUT> out) throws Exception;
}
