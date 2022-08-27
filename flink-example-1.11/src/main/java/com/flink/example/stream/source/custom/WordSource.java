package com.flink.example.stream.source.custom;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

/**
 * 功能：输出单词的 Source 每次递增多输出一个单词
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2022/8/27 下午9:08
 */
public class WordSource extends RichParallelSourceFunction<String> {

    private int sleepInterval = 1000;
    private int sleepMax = 3;
    private int sleepIndex = 1;
    private volatile boolean cancel;
    // 每一次递增一个 初始为1个
    private int wordNum = 1;
    // 输出的单词
    private String word = "a";


    public WordSource() {
    }

    public WordSource(int sleepInterval, int sleepMax) {
        this.sleepInterval = sleepInterval;
        this.sleepMax = sleepMax;
    }

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        while (!cancel) {
            synchronized (ctx.getCheckpointLock()) {
                for (int i = 0;i < wordNum;i++) {
                    ctx.collect(word);
                }
            }
            Thread.sleep(sleepInterval);
            wordNum ++;
            sleepIndex ++;
            if (sleepIndex > sleepMax) {
                cancel = true;
            }
        }
    }

    @Override
    public void cancel() {
        cancel = true;
    }
}
