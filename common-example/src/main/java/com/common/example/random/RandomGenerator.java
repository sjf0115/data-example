package com.common.example.random;

import org.apache.commons.math3.random.RandomDataGenerator;

import java.io.Serializable;
import java.util.Iterator;

/**
 * 功能：随机生成器
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2025/1/1 20:36
 */
public abstract class RandomGenerator<T> implements Iterator<T>, Serializable {
    protected transient RandomDataGenerator random;

    public RandomGenerator() {
        this.random = new RandomDataGenerator();
    }

    public boolean hasNext() {
        return true;
    }

    public static RandomGenerator<Long> longGenerator(long min, long max) {
        return new RandomGenerator<Long>() {
            @Override
            public Long next() {
                return random.nextLong(min, max);
            }
        };
    }

    public static RandomGenerator<Integer> intGenerator(int min, int max) {
        return new RandomGenerator<Integer>() {
            @Override
            public Integer next() {
                return random.nextInt(min, max);
            }
        };
    }

    public static RandomGenerator<Short> shortGenerator(short min, short max) {
        return new RandomGenerator<Short>() {
            @Override
            public Short next() {
                return (short) random.nextInt(min, max);
            }
        };
    }

    public static RandomGenerator<Byte> byteGenerator(byte min, byte max) {
        return new RandomGenerator<Byte>() {
            @Override
            public Byte next() {
                return (byte) random.nextInt(min, max);
            }
        };
    }

    public static RandomGenerator<Float> floatGenerator(float min, float max) {
        return new RandomGenerator<Float>() {
            @Override
            public Float next() {
                return (float) random.nextUniform(min, max);
            }
        };
    }

    public static RandomGenerator<Double> doubleGenerator(double min, double max) {
        return new RandomGenerator<Double>() {
            @Override
            public Double next() {
                return random.nextUniform(min, max);
            }
        };
    }

    public static RandomGenerator<String> stringGenerator(int len) {
        return new RandomGenerator<String>() {
            @Override
            public String next() {
                return random.nextHexString(len);
            }
        };
    }

    public static RandomGenerator<Boolean> booleanGenerator() {
        return new RandomGenerator<Boolean>() {
            @Override
            public Boolean next() {
                return random.nextInt(0, 1) == 0;
            }
        };
    }
}
