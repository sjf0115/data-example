package com.common.example.generics;

/**
 * 功能：长方形
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2021/10/6 下午2:48
 */
public class Rectangle extends Shape {

    private double len;
    private double wide;

    public Rectangle(double len, double wide) {
        this.len = len;
        this.wide = wide;
    }

    @Override
    public double getArea() {
        return len * wide;
    }
}
