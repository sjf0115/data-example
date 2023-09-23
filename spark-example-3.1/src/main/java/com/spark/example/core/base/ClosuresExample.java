package com.spark.example.core.base;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * 功能：闭包
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2023/9/16 15:38
 */
public class ClosuresExample {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("ClosuresExample").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

//        int counter = 0;
//        JavaRDD<Integer> rdd = sc.parallelize(data);
//
//        // Wrong: Don't do this!!
//        rdd.foreach(x -> counter += x);
//
//        System.out.println("Counter value: " + counter);
    }
}
