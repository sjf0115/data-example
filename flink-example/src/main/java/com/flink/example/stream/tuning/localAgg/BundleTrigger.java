package com.flink.example.stream.tuning.localAgg;

import java.io.Serializable;

// 批次触发器
public interface BundleTrigger<T> extends Serializable {
    // 如果结束攒批注册一个回调函数
    void registerCallback(BundleTriggerCallback callback);
    // 每个元素到达时调用该函数
    void onElement(final T element) throws Exception;
    // 重置触发器
    void reset();
}
