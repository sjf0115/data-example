package com.flink.example.stream.tuning.localAgg;

// 批次触发器回调
public interface BundleTriggerCallback {
    // 当触发器触发时结束当前批次
    void finishBundle() throws Exception;
}
