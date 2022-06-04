package com.common.example.jmx.bean;

/**
 * 功能：Game MBean
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/6/4 下午11:24
 */
public interface CounterMBean {
    public int getCounter();
    public void setCounter(int counter);
    // 增加1
    public void increase();
    // 降低1
    public void decrease();
}
