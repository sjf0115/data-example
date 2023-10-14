package com.spark.example.sql;

import com.spark.example.bean.Person;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;

/**
 * 功能：RDD 与 DataFrame 交互 - 使用反射推导schema
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2023/10/14 14:48
 */
public class DataFrameInferSchemaExample {
    public static void main(String[] args) throws AnalysisException {
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .master("local[*]")
                .getOrCreate();

        runInferSchemaExample(spark);

        spark.stop();
    }

    private static void runInferSchemaExample(SparkSession spark) {
        // 从文本文件中创建 Person 对象的 RDD
        JavaRDD<Person> peopleRDD = spark.read()
                .textFile("spark-example-3.1/src/main/resources/data/people.csv")
                .javaRDD()
                .map(line -> {
                    String[] parts = line.split(",");
                    Person person = new Person();
                    person.setName(parts[0]);
                    person.setAge(Integer.parseInt(parts[1].trim()));
                    return person;
                });

        // 在 JavaBean 的 RDD 上应用 schema 生成 DataFrame
        Dataset<Row> peopleDF = spark.createDataFrame(peopleRDD, Person.class);
        // 注册为临时视图
        peopleDF.createOrReplaceTempView("people");

        // 运行 SQL 获取 DataFrame
        Dataset<Row> teenagersDF = spark.sql("SELECT name FROM people WHERE age BETWEEN 13 AND 19");

        // Row 中的列可以通过字段索引获取
        Encoder<String> stringEncoder = Encoders.STRING();
        Dataset<String> teenagerNamesByIndexDF = teenagersDF.map(
                (MapFunction<Row, String>) row -> "Name: " + row.getString(0),
                stringEncoder);
        teenagerNamesByIndexDF.show();
        // +------------+
        // |       value|
        // +------------+
        // |Name: Justin|
        // +------------+

        // Row 中的列可以通过字段名称获取
        Dataset<String> teenagerNamesByFieldDF = teenagersDF.map(
                (MapFunction<Row, String>) row -> "Name: " + row.<String>getAs("name"),
                stringEncoder);
        teenagerNamesByFieldDF.show();
        // +------------+
        // |       value|
        // +------------+
        // |Name: Justin|
        // +------------+
    }
}
