package com.common.example.random;

/**
 * 功能：Math.random
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2021/12/31 下午12:32
 */
public class MathRandom {
    // Math.random()默认产生大于等于0.0且小于1.0之间的随机double型随机数

    /**
     * 产生 [min, max] 范围内的随机数
     * @param min
     * @param max
     * @return
     */
    public static int getRandomInt(int min, int max) {

        // 1. [min, max)
        // int random = min + (int)(Math.random() * (max - min));

        // 2. (min, max]
        // int random = max - (int)(Math.random() * (max - min));

        // 3. (min, max)
        // int random = (int)((max - Math.random()) % max);

        // 4. [min, max]
        int random = min + (int)(Math.random() * (max - min));
        return random;
    }

    public static double getRandomDouble(double min, double max) {

        // 1. [min, max)
        // double random = min + Math.random() * (max - min);

        // 2. (min, max]
        // double random = max - Math.random() * (max - min);

        // 3. (min, max)
        // double random = (max - Math.random()) % max;

        // 4. [min, max]
        double random = min + Math.random() * max % (max - min + 1);
        return random;
    }

    public static void main(String[] args) {
        double min = 0.01;
        double max = 1.708235294117647;

        for (int i = 0; i < 100;i ++) {
            System.out.println(getRandomDouble(min, max));
        }

        int minInt = 1;
        int maxInt = 170;

        for (int i = 0; i < 100;i ++) {
            System.out.println(getRandomInt(minInt, maxInt));
        }
    }

}
