package com.common.example.jmx.bean;
/**
 * 功能：
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/6/4 下午11:28
 */
public class Counter implements CounterMBean {
    private int counter = 0;

    @Override
    public int getCounter() {
        return counter;
    }

    @Override
    public void setCounter(int counter) {
        this.counter = counter;
    }

    // 加1
    @Override
    public void increase() {
        this.counter += 1;
    }

    // 减1
    @Override
    public void decrease() {
        this.counter -= 1;
    }
}
