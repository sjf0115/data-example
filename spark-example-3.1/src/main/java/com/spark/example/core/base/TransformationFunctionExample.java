package com.spark.example.core.base;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * 功能：Transformation 函数
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2023/9/23 16:38
 */
public class TransformationFunctionExample {

    // map
    private static void mapFunction(JavaSparkContext sc) {
        List<String> aList = Lists.newArrayList("a", "B", "c", "b");
        JavaRDD<String> rdd = sc.parallelize(aList);
        // 小写转大写
        JavaRDD<String> upperLinesRDD = rdd.map(new Function<String, String>() {
            @Override
            public String call(String str) throws Exception {
                if (StringUtils.isBlank(str)) {
                    return str;
                }
                return str.toUpperCase();
            }
        });
        Object[] array = upperLinesRDD.collect().toArray();
        for (Object o : array) {
            System.out.println(o);
        }
    }

    // filter
    private static void filterFunction(JavaSparkContext sc) {
        List<String> list = Lists.newArrayList("apple", "banana", "apricot");
        JavaRDD<String> rdd = sc.parallelize(list);
        // 只返回以a开头的水果
        JavaRDD<String> filterRDD = rdd.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String str) throws Exception {
                return str.startsWith("a");
            }
        });
        Object[] array = filterRDD.collect().toArray();
        for (Object o : array) {
            System.out.println(o);
        }
    }

    // flatMap
    private static void flatMapFunction(JavaSparkContext sc) {
        List<String> list = Lists.newArrayList("I am a student", "I like eating apple");
        JavaRDD<String> rdd = sc.parallelize(list);
        // 拆分单词
        JavaRDD<String> resultRDD = rdd.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                if (StringUtils.isBlank(s)) {
                    return null;
                }
                String[] array = s.split(" ");
                return Arrays.asList(array).iterator();
            }
        });
        Object[] array = resultRDD.collect().toArray();
        for (Object o : array) {
            System.out.println(o);
        }
    }

    // distinct
    private static void distinctFunction(JavaSparkContext sc) {
        List<String> aList = Lists.newArrayList("1", "3", "2", "3");
        JavaRDD<String> aRDD = sc.parallelize(aList);
        // 去重
        JavaRDD<String> rdd = aRDD.distinct();
        Object[] array = rdd.collect().toArray();
        for (Object o : array) {
            System.out.println(o);
        }
    }

    // union
    private static void unionFunction(JavaSparkContext sc) {
        List<String> aList = Lists.newArrayList("1", "2", "3");
        List<String> bList = Lists.newArrayList("3", "4", "5");
        JavaRDD<String> aRDD = sc.parallelize(aList);
        JavaRDD<String> bRDD = sc.parallelize(bList);
        // 并集
        JavaRDD<String> rdd = aRDD.union(bRDD); // 1 2 3 3 4 5
        Object[] array = rdd.collect().toArray();
        for (Object o : array) {
            System.out.println(o);
        }
    }

    // intersection
    private static void intersectionFunction(JavaSparkContext sc) {
        List<String> aList = Lists.newArrayList("2", "3", "6");
        List<String> bList = Lists.newArrayList("3", "4", "5");
        JavaRDD<String> aRDD = sc.parallelize(aList);
        JavaRDD<String> bRDD = sc.parallelize(bList);
        // 交集
        JavaRDD<String> rdd = aRDD.intersection(bRDD);
        Object[] array = rdd.collect().toArray();
        for (Object o : array) {
            System.out.println(o);
        }
    }

    // subtract
    private static void subtractFunction(JavaSparkContext sc) {
        List<String> aList = Lists.newArrayList("1", "3", "4");
        List<String> bList = Lists.newArrayList("3", "4", "5");
        JavaRDD<String> aRDD = sc.parallelize(aList);
        JavaRDD<String> bRDD = sc.parallelize(bList);
        // 差集
        JavaRDD<String> rdd = aRDD.subtract(bRDD);
        Object[] array = rdd.collect().toArray();
        for (Object o : array) {
            System.out.println(o);
        }
    }

    // groupByKey
    private static void groupByKeyFunction(JavaSparkContext sc) {
        Tuple2<String, Integer> t1 = new Tuple2("Banana", 10);
        Tuple2<String, Integer> t2 = new Tuple2("Pear", 5);
        Tuple2<String, Integer> t3 = new Tuple2("Banana", 9);
        Tuple2<String, Integer> t4 = new Tuple2("Apple", 4);
        List<Tuple2<String, Integer>> list = Lists.newArrayList();
        list.add(t1);
        list.add(t2);
        list.add(t3);
        list.add(t4);
        JavaPairRDD<String, Integer> rdd = sc.parallelizePairs(list);
        // 分组
        JavaPairRDD<String, Iterable<Integer>> groupRDD = rdd.groupByKey();
        Object[] array = groupRDD.collect().toArray();
        for (Object o : array) {
            System.out.println(o);
        }
    }

    // reduceByKey
    private static void reduceByKeyFunction(JavaSparkContext sc) {
        Tuple2<String, Integer> t1 = new Tuple2("Banana", 10);
        Tuple2<String, Integer> t2 = new Tuple2("Pear", 5);
        Tuple2<String, Integer> t3 = new Tuple2("Banana", 9);
        Tuple2<String, Integer> t4 = new Tuple2("Apple", 4);
        List<Tuple2<String, Integer>> list = Lists.newArrayList();
        list.add(t1);
        list.add(t2);
        list.add(t3);
        list.add(t4);
        JavaPairRDD<String, Integer> rdd = sc.parallelizePairs(list);
        // 分组计算
        JavaPairRDD<String, Integer> reduceRDD = rdd.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        Object[] array = reduceRDD.collect().toArray();
        for (Object o : array) {
            System.out.println(o);
        }
    }

    // sortByKey
    private static void sortByKeyFunction(JavaSparkContext sc) {
        Tuple2<String, Integer> t1 = new Tuple2<String, Integer>("Banana", 10);
        Tuple2<String, Integer> t2 = new Tuple2<String, Integer>("Pear", 5);
        Tuple2<String, Integer> t3 = new Tuple2<String, Integer>("Apple", 4);
        List<Tuple2<String, Integer>> list = Lists.newArrayList();
        list.add(t1);
        list.add(t2);
        list.add(t3);
        JavaPairRDD<String, Integer> rdd = sc.parallelizePairs(list);
        // 根据key排序
        JavaPairRDD<String, Integer> sortRDD = rdd.sortByKey(true);
        Object[] array = sortRDD.collect().toArray();
        for (Object o : array) {
            System.out.println(o);
        }
    }

    // coalesce
    private static void coalesceFunction(JavaSparkContext sc) {
        List<String> list = Lists.newArrayList("1", "2", "3", "4");
        JavaRDD<String> rdd = sc.parallelize(list);
        System.out.println("原始分区个数:" + rdd.partitions().size()); // 4

        JavaRDD<String> coalesceRDD = rdd.coalesce(5, false);
        System.out.println("新分区个数:" + coalesceRDD.partitions().size()); // 4

        JavaRDD<String> coalesceRDD2 = rdd.coalesce(5, true);
        System.out.println("新分区个数:" + coalesceRDD2.partitions().size()); // 5
    }

    // repartition
    private static void repartitionFunction(JavaSparkContext sc) {
        List<String> list = Lists.newArrayList("1", "2", "3", "4");
        JavaRDD<String> rdd = sc.parallelize(list);
        System.out.println("原始分区个数:" + rdd.partitions().size()); // 4
        JavaRDD<String> coalesceRDD = rdd.repartition(2);
        System.out.println("分区个数:" + coalesceRDD.partitions().size()); // 2
    }

    // cartesian
    private static void cartesianFunction(JavaSparkContext sc) {
        List<String> aList = Lists.newArrayList("1", "2");
        List<String> bList = Lists.newArrayList("3", "4");
        JavaRDD<String> aRDD = sc.parallelize(aList);
        JavaRDD<String> bRDD = sc.parallelize(bList);
        // 笛卡尔积
        JavaPairRDD<String, String> cartesianRDD = aRDD.cartesian(bRDD);
        Object[] array = cartesianRDD.collect().toArray();
        for (Object o : array) {
            System.out.println(o);
        }
    }

    // cogroup
    private static void cogroupFunction(JavaSparkContext sc) {
        Tuple2<String, Integer> t1 = new Tuple2<String, Integer>("Banana", 10);
        Tuple2<String, Integer> t2 = new Tuple2<String, Integer>("Pear", 5);
        Tuple2<String, Integer> t3 = new Tuple2<String, Integer>("Apple", 4);

        Tuple2<String, Integer> t4 = new Tuple2<String, Integer>("Banana", 2);
        Tuple2<String, Integer> t5 = new Tuple2<String, Integer>("Apple", 11);
        Tuple2<String, Integer> t6 = new Tuple2<String, Integer>("Banana", 7);
        List<Tuple2<String, Integer>> list1 = Lists.newArrayList();
        list1.add(t1);
        list1.add(t2);
        list1.add(t3);
        List<Tuple2<String, Integer>> list2 = Lists.newArrayList();
        list2.add(t4);
        list2.add(t5);
        list2.add(t6);

        JavaPairRDD<String, Integer> rdd1 = sc.parallelizePairs(list1);
        JavaPairRDD<String, Integer> rdd2 = sc.parallelizePairs(list2);

        JavaPairRDD<String, Tuple2<Iterable<Integer>, Iterable<Integer>>> cogroupRDD = rdd1.cogroup(rdd2);
        cogroupRDD.foreach(new VoidFunction<Tuple2<String, Tuple2<Iterable<Integer>, Iterable<Integer>>>>() {
            @Override
            public void call(Tuple2<String, Tuple2<Iterable<Integer>, Iterable<Integer>>> group) throws Exception {
                String key = group._1;
                Tuple2<Iterable<Integer>, Iterable<Integer>> value = group._2;
                System.out.println(key + " --- " + value.toString());
            }
        });
    }

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("TransformationFunctionExample").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Map
        // mapFunction(sc);
        // Filter
        // filterFunction(sc);
        // FlatMap
        // flatMapFunction(sc);
        // Distinct
        // distinctFunction(sc);
        // Union
        // unionFunction(sc);
        // Intersection
        // intersectionFunction(sc);
        // Subtract
        // subtractFunction(sc);
        // GroupByKey
        // groupByKeyFunction(sc);
        // ReduceByKey
        // reduceByKeyFunction(sc);
        // sortByKey
        // sortByKeyFunction(sc);
        // Coalesce
        // coalesceFunction(sc);
        // Repartition
        // repartitionFunction(sc);
        // Cartesian
        cartesianFunction(sc);
    }
}
