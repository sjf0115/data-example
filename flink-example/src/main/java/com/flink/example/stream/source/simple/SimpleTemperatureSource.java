package com.flink.example.stream.source.simple;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.joda.time.DateTime;

import java.util.Random;

import static com.common.example.utils.TimeUtil.DATE_FORMAT;

/**
 * 功能：随机输出温度 Source
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2022/9/2 下午10:35
 */
public class SimpleTemperatureSource extends RichParallelSourceFunction<Tuple2<String, Integer>> {
    // Sleep 时间间隔 默认 1s
    private Long sleepInterval = 1000L;
    private Random random = new Random();
    private volatile boolean cancel;
    private int minTemperature = 20;
    private int maxTemperature = 30;
    private int days = 10;
    private DateTime startDay = new DateTime();

    public SimpleTemperatureSource() {
    }

    public SimpleTemperatureSource(Long sleepInterval) {
        this.sleepInterval = sleepInterval;
    }

    public SimpleTemperatureSource(int minTemperature, int maxTemperature, int days) {
        this.minTemperature = minTemperature;
        this.maxTemperature = maxTemperature;
        this.days = days;
    }

    public SimpleTemperatureSource(int minTemperature, int maxTemperature, Long sleepInterval) {
        this.sleepInterval = sleepInterval;
        this.minTemperature = minTemperature;
        this.maxTemperature = maxTemperature;
    }

    @Override
    public void run(SourceContext<Tuple2<String, Integer>> ctx) throws Exception {
        int index = 0;
        while (!cancel) {
            synchronized (ctx.getCheckpointLock()) {
                // 随机温度
                int temperature = random.nextInt(maxTemperature - minTemperature + 1) + minTemperature;
                String nextDay = startDay.plusDays(index++).toString(DATE_FORMAT);
                ctx.collect(Tuple2.of(nextDay, temperature));
            }
            if (index > days) {
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
