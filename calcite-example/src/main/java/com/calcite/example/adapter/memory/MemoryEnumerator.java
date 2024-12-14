package com.calcite.example.adapter.memory;

import org.apache.calcite.linq4j.Enumerator;

/**
 * 功能：内存对象读取迭代器
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2024/11/24 16:14
 */
public class MemoryEnumerator<E> implements Enumerator<E> {
    @Override
    public E current() {
        return null;
    }

    @Override
    public boolean moveNext() {
        return false;
    }

    @Override
    public void reset() {

    }

    @Override
    public void close() {

    }
}
