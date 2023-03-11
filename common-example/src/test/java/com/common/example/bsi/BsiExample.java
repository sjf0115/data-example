package com.common.example.bsi;

import com.beust.jcommander.internal.Maps;
import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;
import org.roaringbitmap.IntConsumer;
import org.roaringbitmap.RoaringBitmap;

import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

/**
 * 功能：BSI 测试
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2023/3/9 下午11:21
 */
public class BsiExample {

    private RoaringBitmapSliceIndex bsi;
    private Map<Integer, Integer> input;

    @Before
    public void setup() {
        input = Maps.newHashMap(
                1, 48,
                2, 80,
                3, 75,
                4, 19,
                5, 1,
                6, 57,
                7, 63,
                8, 22,
                9, 96,
                10, 34
        );
        bsi = new RoaringBitmapSliceIndex(1, 96);
        // 添加
        for (Integer key : input.keySet()) {
            bsi.addValue(key, input.get(key));
        }
    }

    @Test
    public void testBitCount() {
        // 切片个数
        System.out.println("切片个数: " + bsi.bitCount());
    }

    @Test
    public void testGet() {
        // 查询
        for (Integer key : input.keySet()) {
            Pair<Integer, Boolean> value = bsi.getValue(key);
            System.out.println(key + ": " + value.left);
        }
    }

    @Test
    public void testGetSliceBitmap() {
        // 输出明细
        Map<Integer, List<Integer>> map = bsi.toMap();
        for (Integer key : map.keySet()) {
            System.out.println(key + ": " + map.get(key).toString());
        }
    }

    @Test
    public void testEQ() {
        // 等于
        RoaringBitmap bitmap = bsi.eq(75);
        bitmap.forEach(new IntConsumer() {
            @Override
            public void accept(int key) {
                System.out.println("key: " + input.get(key));
            }
        });
    }

    @Test
    public void testNEQ() {
        // 不等于
        RoaringBitmap bitmap = bsi.neq(75);
        bitmap.forEach(new IntConsumer() {
            @Override
            public void accept(int key) {
                System.out.println("key: " + input.get(key));
            }
        });
    }

    @Test
    public void testLT() {
        // 小于
        RoaringBitmap bitmap = bsi.lt(75);
        bitmap.forEach(new IntConsumer() {
            @Override
            public void accept(int key) {
                System.out.println("key: " + input.get(key));
            }
        });
    }

    @Test
    public void testLTE() {
        // 小于等于
        RoaringBitmap bitmap = bsi.lte(75);
        bitmap.forEach(new IntConsumer() {
            @Override
            public void accept(int key) {
                System.out.println("key: " + input.get(key));
            }
        });
    }

    @Test
    public void testGT() {
        // 大于
        RoaringBitmap bitmap = bsi.gt(75);
        bitmap.forEach(new IntConsumer() {
            @Override
            public void accept(int key) {
                System.out.println("key: " + input.get(key));
            }
        });
    }

    @Test
    public void testGTE() {
        // 大于等于
        RoaringBitmap bitmap = bsi.gte(75);
        bitmap.forEach(new IntConsumer() {
            @Override
            public void accept(int key) {
                System.out.println("key: " + input.get(key));
            }
        });
    }

    @Test
    public void testRange() {
        // 范围 [start, end]
        RoaringBitmap bitmap = bsi.range(57, 75);
        bitmap.forEach(new IntConsumer() {
            @Override
            public void accept(int key) {
                System.out.println("key: " + input.get(key));
            }
        });
    }
}
