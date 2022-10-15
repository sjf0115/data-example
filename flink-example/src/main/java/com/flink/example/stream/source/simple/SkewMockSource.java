package com.flink.example.stream.source.simple;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * 功能：模拟数据倾斜
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2022/10/14 下午10:57
 */
public class SkewMockSource extends RichParallelSourceFunction<Tuple2<String, Integer>> {

    private static final Logger LOG = LoggerFactory.getLogger(SkewMockSource.class);
    // 速度 每秒多少条
    private long speed = 1000;
    // 阈值 最多发送多条跳
    private int threshold = 500000;
    private volatile boolean cancel = false;
    // flink 是其他单词的3倍
    private List<String> words = Lists.newArrayList("flink", "storm", "flink", "spark", "flink");

    public SkewMockSource() {
    }

    public SkewMockSource(int threshold) {
        this.threshold = threshold;
    }

    public SkewMockSource(int speed, int threshold) {
        this.speed = speed;
        this.threshold = threshold;
    }

    @Override
    public void run(SourceContext<Tuple2<String, Integer>> ctx) throws Exception {
        int index = 0;
        // 每条耗时多少纳秒 1s(1000000000ns)
        long delay = 1000_000_000 / speed;
        long stat1 = System.currentTimeMillis();
        long start = System.nanoTime(); // 纳秒
        while (!cancel) {
            synchronized (ctx.getCheckpointLock()) {
                int wordIndex = index % words.size();
                String word = words.get(wordIndex);
                LOG.info("index:{}, word: {}", index, word);
                ctx.collect(Tuple2.of(word, 1));
            }
            long end = System.nanoTime();
            long diff = end - start; // 耗时
            while (diff < delay) {
                Thread.sleep(1);
                end = System.nanoTime();
                diff = end - start;
            }
            start = end;
            if(index++ >= threshold) {
                long end1 = System.currentTimeMillis();
                System.out.println((end1-stat1) / 1000);
                break;
            }
        }
    }

    @Override
    public void cancel() {
        cancel = true;
    }
}
