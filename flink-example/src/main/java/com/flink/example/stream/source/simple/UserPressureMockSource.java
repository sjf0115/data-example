package com.flink.example.stream.source.simple;

import com.common.example.bean.LoginUser;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

/**
 * 功能：模拟计算 DAU 数据
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2022/10/14 下午10:57
 */
public class UserPressureMockSource extends RichParallelSourceFunction<LoginUser> {

    private static final Logger LOG = LoggerFactory.getLogger(SkewUserMockSource.class);
    // 速度 每秒多少条
    private long speed = 1000;
    // 阈值 最多发送多条跳
    private int threshold = 30000000;
    private volatile boolean cancel = false;
    private Random random = new Random();
    private int minUid = 10000001;
    private int maxUid = 99999999;

    public UserPressureMockSource() {
    }

    public UserPressureMockSource(int threshold) {
        this.threshold = threshold;
    }

    public UserPressureMockSource(int speed, int threshold) {
        this.speed = speed;
        this.threshold = threshold;
    }

    @Override
    public void run(SourceContext<LoginUser> ctx) throws Exception {
        int index = 0;
        // 每条耗时多少纳秒 1s(1000000000ns)
        long delay = 1000_000_000 / speed;
        long stat1 = System.currentTimeMillis();
        long start = System.nanoTime(); // 纳秒
        while (!cancel) {
            synchronized (ctx.getCheckpointLock()) {
                // [minUid, maxUid]
                int uid = random.nextInt(maxUid - minUid + 1) + minUid;
                ctx.collect(new LoginUser(10001, new Long(uid)));
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
