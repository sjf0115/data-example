package com.spark.example.streaming.connector.receiver;

import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

/**
 * 功能：随机生成器Receiver
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2025/1/1 20:42
 */
public class DataGeneratorReceiver<T> extends Receiver implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(DataGeneratorReceiver.class);
    private final long rowsPerSecond;
    private final Long numberOfRows;
    private final RandomDataGenerator random;
    private transient int outputSoFar;

    public DataGeneratorReceiver(long rowsPerSecond) {
        this(rowsPerSecond, null);
    }

    public DataGeneratorReceiver(long rowsPerSecond, Long numberOfRows) {
        super(StorageLevel.MEMORY_AND_DISK_2());
        this.rowsPerSecond = rowsPerSecond;
        this.numberOfRows = numberOfRows == null ? Long.MAX_VALUE : numberOfRows;
        this.random = new RandomDataGenerator();
    }

    @Override
    public void onStart() {
        new Thread(this::run).start();
    }

    @Override
    public void onStop() {
        LOG.info("generated {} rows", outputSoFar);
    }

    private void run() {
        // 当前开始时间
        long nextReadTime = System.currentTimeMillis();
        while (!isStopped()) {
            LOG.info("开始生成记录，计划开始时间：{}， 实际开始时间：{}", nextReadTime, System.currentTimeMillis());
            // 每秒生成 rowsPerSecond 行记录
            for (int i = 0; i < rowsPerSecond; i++) {
                if (outputSoFar < numberOfRows) {
                    // 没有到达指定行数继续输出
                    outputSoFar++;
                    store(random.nextHexString(5));
                } else {
                    return;
                }
            }

            // 下一次开始时间
            nextReadTime += 1000;
            long toWaitMs = nextReadTime - System.currentTimeMillis(); // 等待的剩余时间
            while (toWaitMs > 0) {
                try {
                    Thread.sleep(toWaitMs);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                toWaitMs = nextReadTime - System.currentTimeMillis();
            }
        }
    }
}
