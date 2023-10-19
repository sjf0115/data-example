package com.spark.example.sql.base;

import org.apache.spark.sql.*;
import static org.apache.spark.sql.functions.col;

/**
 * 功能：DataFrame 基本操作
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2023/10/11 07:29
 */
public class BasicDataFrameExample {

    public static void main(String[] args) throws AnalysisException {
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .master("local[*]")
                .getOrCreate();

        runBasicDataFrameExample(spark);
        spark.stop();
    }

    private static void runBasicDataFrameExample(SparkSession spark) throws AnalysisException {
        // 1. 创建 DataFrame
        Dataset<Row> df = spark.read().json("spark-example-3.1/src/main/resources/data/people.txt");

        // 打印 DataFrame 内容
        df.show();
        // +----+-------+
        // | age|   name|
        // +----+-------+
        // |null|Michael|
        // |  30|   Andy|
        // |  19| Justin|
        // +----+-------+

        // Print the schema in a tree format
        df.printSchema();
        // root
        // |-- age: long (nullable = true)
        // |-- name: string (nullable = true)


        // 2. DataFrame 操作

        // 2.1 选择 name 列
        df.select("name").show();
        // +-------+
        // |   name|
        // +-------+
        // |Michael|
        // |   Andy|
        // | Justin|
        // +-------+

        // 2.2 选择name age列 age加一
        df.select(col("name"), col("age").plus(1)).show();
        // +-------+---------+
        // |   name|(age + 1)|
        // +-------+---------+
        // |Michael|     null|
        // |   Andy|       31|
        // | Justin|       20|
        // +-------+---------+

        // 2.3 过滤age大于21岁
        df.filter(col("age").gt(21)).show();
        // +---+----+
        // |age|name|
        // +---+----+
        // | 30|Andy|
        // +---+----+

        // 2.4 按age分组求人数
        df.groupBy("age").count().show();
        // +----+-----+
        // | age|count|
        // +----+-----+
        // |  19|    1|
        // |null|    1|
        // |  30|    1|
        // +----+-----+

        // 2.5 注册 DataFrame 为 SQL 临时视图
        df.createOrReplaceTempView("people");

        Dataset<Row> sqlDF = spark.sql("SELECT * FROM people");
        sqlDF.show();
        // +----+-------+
        // | age|   name|
        // +----+-------+
        // |null|Michael|
        // |  30|   Andy|
        // |  19| Justin|
        // +----+-------+

        // 2.6 注册 DataFrame 为 SQL 全局临时视图
        df.createGlobalTempView("people");

        // 全局临时视图与系统保留的数据库global_temp关联
        spark.sql("SELECT * FROM global_temp.people").show();
        // +----+-------+
        // | age|   name|
        // +----+-------+
        // |null|Michael|
        // |  30|   Andy|
        // |  19| Justin|
        // +----+-------+

        // 全局临时视图是跨会话的
        spark.newSession().sql("SELECT * FROM global_temp.people").show();
        // +----+-------+
        // | age|   name|
        // +----+-------+
        // |null|Michael|
        // |  30|   Andy|
        // |  19| Justin|
        // +----+-------+
    }
}
