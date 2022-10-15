package com.kafka.example.utils;

import com.google.common.collect.Lists;

import java.util.List;

/**
 * 功能：速度验证
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2022/10/14 下午11:15
 */
public class SpeedTest {

    private static List<String> words = Lists.newArrayList("flink", "storm", "flink", "spark", "flink");

    public static void main(String[] args) throws InterruptedException {
        // 速度 每秒多少条
        long speed = 100;
        // 阈值 最多发送多条跳
        int threshold = 10000;
        int index = 0;
        // 每条耗时多少纳秒 1s(1000000ns)
        long delay = 1000000 / speed;
        long stat1 = System.currentTimeMillis();
        long start = System.nanoTime(); // 纳秒
        while (true) {
            int wordIndex = index % words.size();
            String word = words.get(wordIndex);
            System.out.println(index + ":" + word);
            long end = System.nanoTime();
            long diff = end - start; // 耗时
            while (diff < delay*1000) {
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
}
