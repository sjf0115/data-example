package com.flink.example.stream.source.simple;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Random;
import java.util.UUID;

/**
 * 简单的自定义Source
 * Created by wy on 2020/12/26.
 */
public class SimpleCustomSource extends RichParallelSourceFunction<Tuple2<String, Integer>> {

    // Sleep 时间间隔 默认 1s
    private Long sleepInterval = 1000L;

    public SimpleCustomSource() {
    }

    public SimpleCustomSource(Long sleepInterval) {
        this.sleepInterval = sleepInterval;
    }

    private Random random = new Random();
    private volatile boolean cancel;

    @Override
    public void run(SourceContext<Tuple2<String, Integer>> ctx) throws Exception {
        while (!cancel) {
            synchronized (ctx.getCheckpointLock()) {
                String uid = UUID.randomUUID().toString().substring(0, 1);
                int value = random.nextInt(100 / 2 - 1) + 1;
                ctx.collect(new Tuple2<>(uid, value));
            }
            Thread.sleep(sleepInterval);
        }
    }

    @Override
    public void cancel() {
        cancel = true;
    }
}
