package com.flink.example.stream.source.simple;

import com.google.common.collect.Lists;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Random;

/**
 * 功能：按顺序输出单词 不支持并发
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2023/05/06 下午10:57
 */
public class StatefulWordSource extends RichSourceFunction<String> implements CheckpointedFunction {
    private static final Logger LOG = LoggerFactory.getLogger(StatefulWordSource.class);
    // Sleep 时间间隔 默认 1s
    private Long sleepInterval = 1000L;
    private volatile boolean cancel;
    private List<String> words = Lists.newArrayList("a", "b", "c", "d", "e", "f", "g");
    // 最多输出多少个单词
    private int count = 20;
    // 当前单词的位置
    private int position = 0;

    public StatefulWordSource() {
    }

    public StatefulWordSource(Long sleepInterval) {
        this.sleepInterval = sleepInterval;
    }

    public StatefulWordSource(Long sleepInterval, int count) {
        this.sleepInterval = sleepInterval;
        this.count = count;
    }

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        int index = 0;
        while (!cancel) {
            synchronized (ctx.getCheckpointLock()) {
                // [0, words个数减一]
                int wordIndex = position == words.size() ? 0 : position;
                String word = words.get(wordIndex);
                LOG.info("word: {}", word);
                ctx.collect(word);
                position++;
            }
            if (index++ > count) {
                cancel();
            }
            Thread.sleep(sleepInterval);
        }
    }

    @Override
    public void cancel() {
        cancel = true;
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {

    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {

    }
}
