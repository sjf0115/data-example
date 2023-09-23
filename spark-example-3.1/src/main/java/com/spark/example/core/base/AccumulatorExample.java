package com.spark.example.core.base;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.LongAccumulator;

import java.util.Arrays;

/**
 * 功能：累加器示例
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2023/9/22 07:41
 */
public class AccumulatorExample {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("AccumulatorExample").setMaster("local[*]");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        // 定义Long型的累加器
        LongAccumulator accum = jsc.sc().longAccumulator();
        jsc.parallelize(Arrays.asList(1, 2, 3, 4)).foreach(x -> accum.add(x));
        System.out.println("个数：" + accum.value());
    }
}
