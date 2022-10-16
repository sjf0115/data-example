package com.flink.example.stream.source.simple;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.UUID;

/**
 * 功能：模拟 COUNT DISTINCT 数据倾斜
 *      数据格式: <类型, 用户ID>
 *       生成 10000
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2022/10/14 下午10:57
 */
public class SkewUserMockSource extends RichParallelSourceFunction<Tuple2<String, String>> {

    private static final Logger LOG = LoggerFactory.getLogger(SkewUserMockSource.class);
    // 速度 每秒多少条
    private long speed = 1000;
    // 阈值 最多发送多条跳
    private int threshold = 50000;
    private volatile boolean cancel = false;
    // pv 是其他类型的3倍
    private List<String> behaviors = Lists.newArrayList("A", "B", "A", "C", "A");

    public SkewUserMockSource() {
    }

    public SkewUserMockSource(int threshold) {
        this.threshold = threshold;
    }

    public SkewUserMockSource(int speed, int threshold) {
        this.speed = speed;
        this.threshold = threshold;
    }

    @Override
    public void run(SourceContext<Tuple2<String, String>> ctx) throws Exception {
        int index = 0;
        // 每条耗时多少纳秒 1s(1000000000ns)
        long delay = 1000_000_000 / speed;
        long stat1 = System.currentTimeMillis();
        long start = System.nanoTime(); // 纳秒
        while (!cancel) {
            synchronized (ctx.getCheckpointLock()) {
                int behaviorIndex = index % behaviors.size();
                String behavior = behaviors.get(behaviorIndex);
                String uid = UUID.randomUUID().toString().toUpperCase();
                LOG.info("index:{}, behavior: {}, uid: {}", index, behavior, uid);
                ctx.collect(Tuple2.of(behavior, uid));
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
                LOG.info("Source 输出总耗时: {}s", (end1-stat1) / 1000);
                break;
            }
        }
    }

    @Override
    public void cancel() {
        cancel = true;
    }
}
