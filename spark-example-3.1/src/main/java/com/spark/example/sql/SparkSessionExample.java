package com.spark.example.sql;

import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.immutable.Map;

/**
 * 功能：SparkSession 示例
 * 作者：SmartSi
 * CSDN博客：https://blog.csdn.net/sunnyyoona
 * 公众号：大数据生态
 * 日期：2022/8/4 下午11:07
 */
public class SparkSessionExample {
    public static void main(String[] args) {
        // 1. 创建 SparkSession
        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("SparkSessionExample")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();

        // 2. 配置 Spark 的运行时属性
        Map<String, String> configMap = spark.conf().getAll();
        Iterator<Tuple2<String, String>> ite = configMap.iterator();
        while (ite.hasNext()) {
            Tuple2<String, String> config = ite.next();
            System.out.println(config._1 + ": " + config._2);
        }

        // 修改运行时属性
        spark.conf().set("spark.app.name", "SparkSession Example");
        String appName = spark.conf().get("spark.app.name");
        System.out.println("AppName: " + appName);

        // 3. 访问 Catalog 元数据
        spark.catalog().listDatabases().show();
        spark.catalog().listTables().show();
    }
}
