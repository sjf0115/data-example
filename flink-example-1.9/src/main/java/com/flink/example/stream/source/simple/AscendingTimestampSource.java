package com.flink.example.stream.source.simple;

import com.common.example.utils.DateUtil;
import com.google.common.collect.Lists;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Random;

/**
 * 功能：AscendingTimestampSource 时间戳单调递增的Source
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2022/8/31 下午11:35
 */
public class AscendingTimestampSource extends RichParallelSourceFunction<Tuple2<String, Long>> {
    private static final Logger LOG = LoggerFactory.getLogger(AscendingTimestampSource.class);
    // Sleep 时间间隔 默认 1s
    private Long sleepInterval = 1000L;
    private int sleepMax = 20;
    private Random random = new Random();
    private volatile boolean cancel;
    private List<String> words = Lists.newArrayList("a");

    public AscendingTimestampSource() {
    }

    public AscendingTimestampSource(Long sleepInterval) {
        this.sleepInterval = sleepInterval;
    }

    @Override
    public void run(SourceContext<Tuple2<String, Long>> ctx) throws Exception {
        int index = 1;
        while (!cancel) {
            if (index > sleepMax) {
                cancel();
            }
            synchronized (ctx.getCheckpointLock()) {
                // 随机生成一个单词
                int wordIndex = random.nextInt(words.size());
                String word = words.get(wordIndex);
                // 单调递增的时间戳
                Long timestamp = System.currentTimeMillis();
                LOG.info("word: {}, timestamp: {}, time: {}", word, timestamp, DateUtil.timeStamp2Date(timestamp));
                ctx.collect(Tuple2.of(word, timestamp));
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