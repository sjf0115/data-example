package com.flink.example.stream.source.simple;

import com.google.common.collect.Lists;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Random;

/**
 * 输出单字母单词的 Source
 * Created by wy on 2020/12/26.
 */
public class SimpleWordSource extends RichParallelSourceFunction<String> {
    private static final Logger LOG = LoggerFactory.getLogger(SimpleWordSource.class);
    // Sleep 时间间隔 默认 1s
    private Long sleepInterval = 1000L;
    private Random random = new Random();
    private volatile boolean cancel;
    private int count = 20;
    private List<String> words = Lists.newArrayList("a", "b");

    public SimpleWordSource() {
    }

    public SimpleWordSource(Long sleepInterval) {
        this.sleepInterval = sleepInterval;
    }

    public SimpleWordSource(Long sleepInterval, int count) {
        this.sleepInterval = sleepInterval;
        this.count = count;
    }

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        int index = 0;
        while (!cancel) {
            synchronized (ctx.getCheckpointLock()) {
                // [0, words个数减一]
                int wordIndex = random.nextInt(words.size());
                String word = words.get(wordIndex);
                LOG.info("word: {}", word);
                ctx.collect(word);
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
}
