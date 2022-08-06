package com.spark.example.sql;

import org.apache.spark.sql.SparkSession;

/**
 * 功能：SparkSession 示例
 * 作者：SmartSi
 * CSDN博客：https://blog.csdn.net/sunnyyoona
 * 公众号：大数据生态
 * 日期：2022/8/4 下午11:07
 */
public class SparkSessionExample {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .master("local[2]")
                .appName("Java Spark SQL basic example")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();

    }
}
