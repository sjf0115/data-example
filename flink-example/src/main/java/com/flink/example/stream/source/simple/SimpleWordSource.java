package com.flink.example.stream.source.simple;

import com.google.common.collect.Lists;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.List;
import java.util.Random;

/**
 * 输出单字母单词的 Source
 * Created by wy on 2020/12/26.
 */
public class SimpleWordSource extends RichParallelSourceFunction<String> {
    // Sleep 时间间隔 默认 1s
    private Long sleepInterval = 1000L;
    private Random random = new Random();
    private volatile boolean cancel;
    private List<String> words = Lists.newArrayList("flink", "spark");

    public SimpleWordSource() {
    }

    public SimpleWordSource(Long sleepInterval) {
        this.sleepInterval = sleepInterval;
    }

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        while (!cancel) {
            synchronized (ctx.getCheckpointLock()) {
                // [0, words个数减一]
                int index = random.nextInt(words.size());
                ctx.collect(words.get(index));
            }
            Thread.sleep(sleepInterval);
        }
    }

    @Override
    public void cancel() {
        cancel = true;
    }
}
