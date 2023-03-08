package com.common.example.test;


import com.google.common.collect.Lists;

import java.util.List;

/**
 * 功能：
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2022/12/31 下午11:07
 */
public class Test {
    public static void main(String[] args) {
        List<Integer> list = Lists.newArrayList(48,80,75,19,1,57,63,22,96,34);
        for (Integer v : list) {
            System.out.println(v + ": " + Integer.toBinaryString(v)+ ", " + Integer.toBinaryString(v).length());
        }
    }
}
