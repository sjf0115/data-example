package com.flink.example.stream.tuning.localAgg;

/**
 * 功能：当批次中的数据记录元素个数到达指定阈值则触发
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2022/10/15 下午11:38
 */
public class CountBundleTrigger<T> implements BundleTrigger<T> {
    private final long maxCount;
    private transient BundleTriggerCallback callback;
    private transient long count = 0;

    public CountBundleTrigger(long maxCount) {
        this.maxCount = maxCount;
    }

    @Override
    public void registerCallback(BundleTriggerCallback callback) {
        this.callback = callback;
    }

    @Override
    public void onElement(T element) throws Exception {
        count++;
        if (count >= maxCount) {
            // 调用回调函数
            callback.finishBundle();
            reset();
        }
    }

    @Override
    public void reset() {
        count = 0;
    }
}
