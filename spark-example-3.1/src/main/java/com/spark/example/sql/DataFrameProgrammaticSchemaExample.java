package com.spark.example.sql;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * 功能：RDD 与 DataFrame 交互 - 使用编程方式指定 Schema
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2023/10/11 07:29
 */
public class DataFrameProgrammaticSchemaExample {

    public static void main(String[] args) throws AnalysisException {
        SparkSession spark = SparkSession
                .builder()
                .appName("programmatic schema example")
                .master("local[*]")
                .getOrCreate();

        runProgrammaticSchemaExample(spark);

        spark.stop();
    }

    private static void runProgrammaticSchemaExample(SparkSession spark) {
        // 1. 原始 RDD
        JavaRDD<String> peopleRDD = spark.sparkContext()
                .textFile("spark-example-3.1/src/main/resources/data/people.csv", 1)
                .toJavaRDD();

        // 2. 将原始 RDD 转化为 Rows 的 RDD
        JavaRDD<Row> rowRDD = peopleRDD.map((Function<String, Row>) record -> {
            String[] attributes = record.split(",");
            return RowFactory.create(attributes[0], attributes[1].trim());
        });

        // 3. 创建 Schema
        String schemaString = "name age";
        List<StructField> fields = new ArrayList<>();
        for (String fieldName : schemaString.split(" ")) {
            StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
            fields.add(field);
        }
        StructType schema = DataTypes.createStructType(fields);

        // 4. 将 Schema 应用到 RDD 上创建 DataFrame
        Dataset<Row> peopleDataFrame = spark.createDataFrame(rowRDD, schema);

        // 在 DataFrame 上创建临时视图
        peopleDataFrame.createOrReplaceTempView("people");

        // 运行 SQL
        Dataset<Row> results = spark.sql("SELECT name FROM people");

        // 通过下标获取 name 字段
        Dataset<String> namesDS = results.map(
                (MapFunction<Row, String>) row -> "Name: " + row.getString(0),
                Encoders.STRING());
        namesDS.show();
        // +-------------+
        // |        value|
        // +-------------+
        // |Name: Michael|
        // |   Name: Andy|
        // | Name: Justin|
        // +-------------+
    }
}
