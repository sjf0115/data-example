package com.flink.example.stream.source.simple;

import com.common.example.utils.DateUtil;
import com.google.common.collect.Lists;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Random;

/**
 * 功能：无序 Source
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2022/9/4 下午10:18
 */
public class DynamicOutOfOrderSource extends RichParallelSourceFunction<Tuple3<String, Integer, Long>> {
    private static final Logger LOG = LoggerFactory.getLogger(DynamicOutOfOrderSource.class);
    // Sleep 时间间隔 默认 1s
    private Long sleepInterval = 1000L;
    private int sleepMax = 20;
    private Random random = new Random();
    private volatile boolean cancel;
    private List<String> words = Lists.newArrayList("a");

    public DynamicOutOfOrderSource() {
    }

    public DynamicOutOfOrderSource(Long sleepInterval) {
        this.sleepInterval = sleepInterval;
    }

    @Override
    public void run(SourceContext<Tuple3<String, Integer, Long>> ctx) throws Exception {
        int index = 1;
        while (!cancel) {
            if (index > sleepMax) {
                cancel();
            }
            synchronized (ctx.getCheckpointLock()) {
                // 随机生成单词
                int wordIndex = random.nextInt(words.size());
                String word = words.get(wordIndex);
                // 随机延迟时间戳 最大延迟15s
                int lateMillis = random.nextInt(15) * 1000;
                // 处理时间戳
                long processTimestamp = System.currentTimeMillis();
                // 事件时间戳
                long eventTimestamp = processTimestamp - lateMillis;
                // 单词出现次数
                int count = random.nextInt(5);
                LOG.info("word: {}, count: {}, processTime: {}|{}, eventTime: {}|{}",
                        word, count,
                        processTimestamp, DateUtil.timeStamp2Date(processTimestamp),
                        eventTimestamp, DateUtil.timeStamp2Date(eventTimestamp)
                );
                ctx.collect(Tuple3.of(word, count, eventTimestamp));
            }
            Thread.sleep(sleepInterval);
            index ++;
        }
    }

    @Override
    public void cancel() {
        cancel = true;
    }
}
