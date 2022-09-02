package com.common.example.random;

import java.util.Random;

/**
 * 功能：java.util.Random
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2022/9/2 下午10:54
 */
public class RandomExample {
    public static void main(String[] args) {
        // 计算公式 （(最大值 - 最小值 + 1) + 最小值）
        Random random = new Random();
        int min = 10;
        int max = 20;
        for (int i = 0;i < 100;i++) {
            System.out.println(random.nextInt(max - min + 1) + min);
        }
    }
}
