package com.spark.example.sql.base;

import com.google.common.collect.Lists;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.immutable.Map;

import java.util.List;

import static org.apache.spark.sql.functions.desc;

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
        //spark.sql.warehouse.dir: file:/Users/wy/study/code/data-example/spark-warehouse/
        //spark.driver.host: 192.168.0.104
        //spark.driver.port: 53172
        //spark.some.config.option: some-value
        //spark.app.name: SparkSessionExample
        //spark.app.startTime: 1697153384635
        //spark.executor.id: driver
        //spark.master: local[*]
        //spark.app.id: local-1697153387386

        // 修改运行时属性
        spark.conf().set("spark.app.name", "SparkSession Example");
        String appName = spark.conf().get("spark.app.name");
        System.out.println("AppName: " + appName);
        // AppName: SparkSession Example

        // 3. 访问 Catalog 元数据
        spark.catalog().listDatabases().show();
        //+-------+----------------+--------------------+
        //|   name|     description|         locationUri|
        //+-------+----------------+--------------------+
        //|default|default database|file:/Users/wy/st...|
        //+-------+----------------+--------------------+
        spark.catalog().listTables().show();
        //+----+--------+-----------+---------+-----------+
        //|name|database|description|tableType|isTemporary|
        //+----+--------+-----------+---------+-----------+
        //+----+--------+-----------+---------+-----------+

        // 4. 创建 Dataset 和 DataFrame
        Dataset<Long> rangeDS = spark.range(5, 100, 5);
        rangeDS.orderBy(desc("id")).show(5);
        //+---+
        //| id|
        //+---+
        //| 95|
        //| 90|
        //| 85|
        //| 80|
        //| 75|
        //+---+
        rangeDS.describe().show();
        //+-------+------------------+
        //|summary|                id|
        //+-------+------------------+
        //|  count|                19|
        //|   mean|              50.0|
        //| stddev|28.136571693556885|
        //|    min|                 5|
        //|    max|                95|
        //+-------+------------------+

        // 5. 使用 SparkSession API 读取 JSON 数据
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

        // 6. 在 SparkSession 中使用 Spark SQL
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
    }
}
