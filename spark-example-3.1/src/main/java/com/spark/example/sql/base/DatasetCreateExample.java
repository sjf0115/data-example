package com.spark.example.sql.base;

import com.spark.example.bean.Person;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import java.util.Arrays;
import java.util.Collections;

/**
 * 功能：DataSet 创建示例
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2023/10/11 07:29
 */
public class DatasetCreateExample {

    public static void main(String[] args) throws AnalysisException {
        SparkSession spark = SparkSession
                .builder()
                .appName("DatasetCreateExample")
                .master("local[*]")
                .getOrCreate();

        runDatasetCreationExample(spark);

        spark.stop();
    }

    private static void runDatasetCreationExample(SparkSession spark) {
        // 创建 Person Bean
        Person person = new Person();
        person.setName("Andy");
        person.setAge(32);

        // 根据 Java Bean 创建 Encoders
        Encoder<Person> personEncoder = Encoders.bean(Person.class);
        Dataset<Person> javaBeanDS = spark.createDataset(
                Collections.singletonList(person),
                personEncoder
        );
        javaBeanDS.show();
        // +---+----+
        // |age|name|
        // +---+----+
        // | 32|Andy|
        // +---+----+

        // 大多数常见类型的编码器都在 Encoders 中提供
        Encoder<Long> longEncoder = Encoders.LONG();
        Dataset<Long> primitiveDS = spark.createDataset(Arrays.asList(1L, 2L, 3L), longEncoder);
        Dataset<Long> transformedDS = primitiveDS.map(
                (MapFunction<Long, Long>) value -> value + 1L,
                longEncoder);
        transformedDS.collect(); // Returns [2, 3, 4]

        // DataFrame 通过 Encoders 可以转换为 DataSet，基于名称映射
        String path = "spark-example-3.1/src/main/resources/data/people.txt";
        Dataset<Person> peopleDS = spark.read().json(path).as(personEncoder);
        peopleDS.show();
        // +----+-------+
        // | age|   name|
        // +----+-------+
        // |null|Michael|
        // |  30|   Andy|
        // |  19| Justin|
        // +----+-------+
    }
}
