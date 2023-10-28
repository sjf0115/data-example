package com.spark.example.sql.datasource;

import org.apache.spark.sql.*;

/**
 * 功能：数据源 Load 与 Save 示例
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2023/10/19 07:57
 */
public class BasicDataSourceExample {
    public static void main(String[] args) throws AnalysisException {
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .master("local[*]")
                .getOrCreate();

        //test(spark);
        //defaultFormat(spark);
        //jsonFormat(spark);
        //csvFormat(spark);
        orcFormat(spark);
        //parquetFormat(spark);
        //runSQLOnFile(spark);
        //saveMode(spark);
        //saveAsTable(spark);
        //bucketBy(spark);
        //partitionBy(spark);
        //bucketAndPartitionBy(spark);
        spark.stop();
    }

    // 测试
    private static void test(SparkSession spark) {
        // 如果不指定 format 默认读取的是 parquet 文件
        Dataset<Row> usersDF = spark.read().load("spark-example-3.1/src/main/resources/data/users.parquet");
        usersDF.show();
        // 如果不指定 format 默认保存的是 parquet 文件
        usersDF.select("name", "favorite_color", "favorite_numbers").write().format("orc").save("users.orc");
    }

    // 默认 Format
    private static void defaultFormat(SparkSession spark) {
        // 如果不指定 format 默认读取的是 parquet 文件
        Dataset<Row> usersDF = spark.read().load("spark-example-3.1/src/main/resources/data/users.parquet");
        usersDF.show();
        // 如果不指定 format 默认保存的是 parquet 文件
        usersDF.select("name", "favorite_color").write().save("namesAndFavColors.parquet");
    }

    // json 格式
    private static void jsonFormat(SparkSession spark) {
        // format 指定为 json 读取的是 json 文件
        Dataset<Row> usersDF = spark.read().format("json").load("spark-example-3.1/src/main/resources/data/users.json");
        usersDF.show();
        // format 指定为 json 保存的是 json 文件
        usersDF.select("name", "favorite_color").write().format("json").save("namesAndFavColors.json");
    }

    // csv 格式
    private static void csvFormat(SparkSession spark) {
        // format 指定为 csv 读取的是 csv 文件 指定额外参数
        Dataset<Row> peopleDF = spark.read().format("csv")
                .option("sep", ";")
                .option("inferSchema", "true")
                .option("header", "true")
                .load("spark-example-3.1/src/main/resources/data/people2.csv");
        peopleDF.show();

        // format 指定为 csv 保存的是 csv 文件
        peopleDF.select("name", "age").write().format("csv")
                .option("sep", ",")
                .option("header", "false")
                .save("namesAndAges.csv");
    }

    // orc 格式
    private static void orcFormat(SparkSession spark) {
        // format 指定为 orc 读取的是 orc 文件
        Dataset<Row> usersDF = spark.read().format("orc").load("spark-example-3.1/src/main/resources/data/users.orc");
        usersDF.show();

        // format 指定为 orc 保存的是 orc 文件
        usersDF.write().format("orc")
                .option("orc.bloom.filter.columns", "favorite_color")
                .option("orc.dictionary.key.threshold", "1.0")
                .option("orc.column.encoding.direct", "name")
                .save("users_with_options.orc");
    }

    // parquet 格式
    private static void parquetFormat(SparkSession spark) {
        // format 指定为 parquet 读取的是 parquet 文件
        Dataset<Row> usersDF = spark.read().format("parquet").load("spark-example-3.1/src/main/resources/data/users.parquet");
        usersDF.show();

        // format 指定为 parquet 保存的是 parquet 文件
        usersDF.write().format("parquet")
                .option("parquet.bloom.filter.enabled#favorite_color", "true")
                .option("parquet.bloom.filter.expected.ndv#favorite_color", "1000000")
                .option("parquet.enable.dictionary", "true")
                .option("parquet.page.write-checksum.enabled", "false")
                .save("users_with_options.parquet");
    }

    // 文件上直接运行 SQL
    private static void runSQLOnFile(SparkSession spark) {
        // 在 parquet 文件上直接运行 SQL
        Dataset<Row> parquetDF = spark.sql("SELECT * FROM parquet.`spark-example-3.1/src/main/resources/data/users.parquet`");
        parquetDF.show();
        // 在 json 文件上直接运行 SQL
        Dataset<Row> jsonDF = spark.sql("SELECT * FROM json.`spark-example-3.1/src/main/resources/data/users.json`");
        jsonDF.show();
    }

    // SaveMode 保存模式
    private static void saveMode(SparkSession spark) {
        // format 指定为 json 读取的是 json 文件
        Dataset<Row> usersDF = spark.read().format("json").load("spark-example-3.1/src/main/resources/data/users.json");
        usersDF.show();
        // 使用 SaveMode 指定保存模式
        usersDF.select("name", "favorite_color").write().format("json")
                .mode(SaveMode.ErrorIfExists)
                .save("namesAndFavColors.json");
    }

    // saveAsTable
    private static void saveAsTable(SparkSession spark) {
        // format 指定为 json 读取的是 json 文件
        Dataset<Row> usersDF = spark.read().format("json").load("spark-example-3.1/src/main/resources/data/users.json");
        usersDF.show();
        // 使用 saveAsTable 指定保存模式
        usersDF.select("name", "favorite_color").write().format("json")
                .mode(SaveMode.Overwrite)
                .saveAsTable("namesAndFavColors");

        // 查询 在 SparkSession 上调用 table 方法来创建 DataFrame 的持久化表
        Dataset<Row> namesAndFavColorsDF = spark.table("namesAndFavColors");
        namesAndFavColorsDF.show();
    }

    // 分桶
    private static void bucketBy(SparkSession spark) {
        // format 指定为 json 读取的是 json 文件
        Dataset<Row> peopleDF = spark.read().format("json").load("spark-example-3.1/src/main/resources/data/people.json");
        peopleDF.show();
        // 根据年龄分桶
        peopleDF.write().format("json")
                .bucketBy(3, "age")
                .sortBy("age")
                .saveAsTable("people_bucketed");
        // 查询
        Dataset<Row> dataset = spark.sql("SELECT * FROM people_bucketed");
        dataset.show();
    }

    // 分区
    private static void partitionBy(SparkSession spark) {
        // format 指定为 json 读取的是 json 文件
        Dataset<Row> usersDF = spark.read().format("json").load("spark-example-3.1/src/main/resources/data/users.json");
        usersDF.show();
        // 根据favorite_color分区
        usersDF.write().format("json")
                .partitionBy("favorite_color")
                .save("namesPartByColor.json");
    }

    // 分区和分桶
    private static void bucketAndPartitionBy(SparkSession spark) {
        // format 指定为 json 读取的是 json 文件
        Dataset<Row> usersDF = spark.read().format("json").load("spark-example-3.1/src/main/resources/data/users.json");
        usersDF.show();
        // 根据姓名分桶根据喜欢颜色分区
        usersDF.write()
                .partitionBy("favorite_color")
                .bucketBy(3, "name")
                .saveAsTable("users_partitioned_bucketed");
        // 查询
        Dataset<Row> dataset = spark.sql("SELECT * FROM users_partitioned_bucketed");
        dataset.show();
    }
}
