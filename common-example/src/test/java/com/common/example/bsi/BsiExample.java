package com.common.example.bsi;

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
    private List<Pair<Integer, Integer>> input;

    @Before
    public void setup() {
        input = Lists.newArrayList(
                Pair.of(1, 48),
                Pair.of(2, 80),
                Pair.of(3, 75),
                Pair.of(4, 19),
                Pair.of(5, 1),
                Pair.of(6, 57),
                Pair.of(7, 63),
                Pair.of(8, 22),
                Pair.of(9, 96),
                Pair.of(10, 34));
        bsi = new RoaringBitmapSliceIndex(1, 96);
        // 添加
        for (Pair<Integer, Integer> pair : input) {
            bsi.addValue(pair.left, pair.right);
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
        for (Pair<Integer, Integer> pair : input) {
            Pair<Integer, Boolean> value = bsi.getValue(pair.left);
            System.out.println(pair.left + ": " + value.left);
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
    public void testLE() {
        RoaringBitmap bitmap = bsi.lte(51);
        bitmap.forEach(new IntConsumer() {
            @Override
            public void accept(int key) {
                System.out.println("Value 小于等于 51 的 Key：" + key);
            }
        });
    }
}
