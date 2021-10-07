package com.common.example.generics;

import java.util.ArrayList;
import java.util.List;

/**
 * 功能：类型擦除 - Bridge
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2021/10/6 上午12:10
 */
public class TypeErasureBridgeExample {
    public static void main(String[] args) {
//        MyNode mn = new MyNode(5);
//        Node n = mn;            // A raw type - compiler throws an unchecked warning
//        n.setData("Hello");     // Causes a ClassCastException to be thrown.
//        Integer x = mn.data;

        List list = new ArrayList();
        list.add("hello");
        String s = (String) list.get(0);
    }
}


