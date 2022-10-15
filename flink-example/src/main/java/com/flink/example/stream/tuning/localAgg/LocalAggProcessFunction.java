package com.flink.example.stream.tuning.localAgg;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

/**
 * 功能：本地聚合处理函数
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2022/10/15 下午10:52
 */
public abstract class LocalAggProcessFunction<K, V, IN, OUT> extends ProcessFunction<IN, OUT>
        implements BundleTriggerCallback {

    private transient Map<K, V> bundle;
    private final BundleTrigger<IN> bundleTrigger;
    private final BundleFunction<K, V, IN, OUT> bundleFunction;
    private transient Collector<OUT> collector;

    public LocalAggProcessFunction(BundleTrigger<IN> bundleTrigger, BundleFunction<K, V, IN, OUT> bundleFunction) {
        this.bundleTrigger = bundleTrigger;
        this.bundleFunction = bundleFunction;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // 批次缓存
        this.bundle = new HashMap<>();
        // 注册批次触发器回调
        bundleTrigger.registerCallback(this);
        bundleTrigger.reset();
    }

    // 处理每个到达的数据记录元素
    @Override
    public void processElement(IN element, Context ctx, Collector<OUT> out) throws Exception {
        // 批次 Key
        final K bundleKey = getKey(element);
        // 批次 Value
        final V bundleValue = bundle.get(bundleKey);
        // 添加一个新数据记录元素到批次中并返回新的批次 Value
        final V newBundleValue = bundleFunction.addElement(bundleValue, element);
        // 更新批次
        bundle.put(bundleKey, newBundleValue);
        // 调用触发器
        bundleTrigger.onElement(element);
        // 保存输出器
        this.collector = out;
    }

    // 结束一个批次
    @Override
    public void finishBundle() throws Exception {
        if (bundle != null && !bundle.isEmpty()) {
            bundleFunction.finishBundle(bundle, collector);
            bundle.clear();
        }
        bundleTrigger.reset();
    }

    // -----------------------------------------------------------------------------------------------------------------
    // 抽象方法 需要实现类实现

    // 数据记录对应的Key
    protected abstract K getKey(final IN element) throws Exception;
}
