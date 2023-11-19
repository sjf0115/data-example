package com.spark.example.bean;

import java.io.Serializable;
import java.util.Comparator;

/**
 * 功能：Comparator 序列化接口
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2023/11/20 07:34
 */
public interface SerializableComparator<T> extends Comparator<T>, Serializable {

}
