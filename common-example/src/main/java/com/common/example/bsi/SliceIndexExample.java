package com.common.example.bsi;

import com.google.common.collect.Lists;

import java.util.List;
import java.util.Map;

/**
 * 功能：
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2023/3/8 下午10:42
 */
public class SliceIndexExample {
    public static void main(String[] args) {
        List<Pair<Integer, Integer>> input = Lists.newArrayList(
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
        // 创建
        RoaringBitmapSliceIndex bsi = new RoaringBitmapSliceIndex(1, 96);
        // 添加
        for (Pair<Integer, Integer> pair : input) {
            bsi.addValue(pair.left, pair.right);
        }

        // 输出切片个数
        System.out.println("切片个数: " + bsi.bitCount());

        // 查询
        for (Pair<Integer, Integer> pair : input) {
            Pair<Integer, Boolean> value = bsi.getValue(pair.left);
            System.out.println(pair.left + ": " + value.left);
        }

        // 输出明细
        Map<Integer, List<Integer>> map = bsi.toMap();
        for (Integer key : map.keySet()) {
            System.out.println(key + ": " + map.get(key).toString());
        }
    }
}
