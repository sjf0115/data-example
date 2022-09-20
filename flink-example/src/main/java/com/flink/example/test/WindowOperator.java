package com.flink.example.test;

import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.runtime.operators.windowing.functions.InternalWindowFunction;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * 功能：
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2022/9/17 下午2:44
 */
public class WindowOperator<K, IN, ACC, OUT, W extends Window>
        extends AbstractUdfStreamOperator<OUT, InternalWindowFunction<ACC, OUT, K, W>>
        implements OneInputStreamOperator<IN, OUT>, Triggerable<K, W> {

    // 1. 继承 AbstractUdfStreamOperator
    public WindowOperator(InternalWindowFunction<ACC, OUT, K, W> userFunction) {
        super(userFunction);
    }

    @Override
    public void open() throws Exception {
        super.open();
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    @Override
    public void dispose() throws Exception {
        super.dispose();
    }

    // 2. 实现 OneInputStreamOperator 接口
    @Override
    public void processElement(StreamRecord<IN> element) throws Exception {

    }

    // 3. 实现 Triggerable 接口
    @Override
    public void onEventTime(InternalTimer<K, W> timer) throws Exception {

    }

    @Override
    public void onProcessingTime(InternalTimer<K, W> timer) throws Exception {

    }
}
