package com.flink.example.stream.source.simple;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.List;
import java.util.Random;

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
    private int count = 20;
    private List<String> sensors = Lists.newArrayList("a", "b", "c");

    public SimpleTemperatureSource() {
    }

    public SimpleTemperatureSource(Long sleepInterval) {
        this.sleepInterval = sleepInterval;
    }

    public SimpleTemperatureSource(int minTemperature, int maxTemperature) {
        this.minTemperature = minTemperature;
        this.maxTemperature = maxTemperature;
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
                int sensorIndex = random.nextInt(sensors.size());
                ctx.collect(Tuple2.of(sensors.get(sensorIndex), temperature));
            }
            if (index > count) {
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
