package com.spark.example.streaming.connector.receiver;

import com.common.example.random.RandomGenerator;
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
public class DataGeneratorReceiver<T> extends Receiver<T> implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(DataGeneratorReceiver.class);
    private final long rowsPerSecond;
    private final Long numberOfRows;
    private RandomGenerator<T> randomGenerator;
    private transient int outputSoFar;

    public DataGeneratorReceiver(RandomGenerator randomGenerator, long rowsPerSecond) {
        this(randomGenerator, rowsPerSecond, null);
    }

    public DataGeneratorReceiver(RandomGenerator randomGenerator, long rowsPerSecond, Long numberOfRows) {
        super(StorageLevel.MEMORY_AND_DISK_2());
        this.rowsPerSecond = rowsPerSecond;
        this.randomGenerator = randomGenerator;
        this.numberOfRows = numberOfRows == null ? Long.MAX_VALUE : numberOfRows;
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
        long nextReadTime = System.currentTimeMillis();
        while (!isStopped()) {
            // 每秒生成 rowsPerSecond 行记录
            for (int i = 0; i < rowsPerSecond; i++) {
                if (randomGenerator.hasNext() && (outputSoFar < numberOfRows)) {
                    // 没有到达指定行数继续输出
                    outputSoFar++;
                    store(this.randomGenerator.next());
                } else {
                    return;
                }
            }

            // 等待
            nextReadTime += 1000;
            long toWaitMs = nextReadTime - System.currentTimeMillis();
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
