package com.common.example.generics;

/**
 * 功能：正方形
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2021/10/6 下午2:46
 */
public class Square extends Shape {
    private double len;

    public Square(double len) {
        this.len = len;
    }

    @Override
    public double getArea() {
        return len*len;
    }
}
