package com.spark.example.core.join;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import scala.Tuple4;

import java.util.ArrayList;
import java.util.List;

/**
 * 功能：JOIN 示例
 * 作者：SmartSi
 * CSDN博客：https://blog.csdn.net/sunnyyoona
 * 公众号：大数据生态
 * 日期：2022/8/3 下午10:50
 */
public class JoinExample {
    public static void main(String[] args) {

        SparkSession session = SparkSession.builder()
                .appName("JoinExample")
                .master("local[*]")
                .getOrCreate();

        // 员工表
        List<Tuple4<Integer, String, Integer, String>> employees = new ArrayList<>();
        employees.add(new Tuple4<>(1, "Mike", 28, "Male"));
        employees.add(new Tuple4<>(2, "Lily", 30, "Female"));
        employees.add(new Tuple4<>(3, "Raymond", 26, "Male"));
        employees.add(new Tuple4<>(5, "Dave", 36, "Male"));

        Dataset<Row> dataFrame = session.createDataFrame(employees, Tuple4.class);

        List<Tuple2<Integer, Integer>> salaries = new ArrayList<>();
        salaries.add(new Tuple2<>(1, 26000));
        salaries.add(new Tuple2<>(2, 30000));
        salaries.add(new Tuple2<>(4, 25000));
        salaries.add(new Tuple2<>(3, 20000));

    }
}
