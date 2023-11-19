package com.spark.example.core.base;

import com.google.common.collect.Lists;
import com.spark.example.bean.Person;
import com.spark.example.bean.SerializableComparator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.util.LongAccumulator;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 * 功能：Action 函数示例
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2023/9/24 07:24
 */
public class ActionFunctionExample implements Serializable {

    // reduce
    private static void reduceFunction(JavaSparkContext sc) {
        List<Integer> aList = Lists.newArrayList(1, 2, 3, 4);
        JavaRDD<Integer> rdd = sc.parallelize(aList);
        Integer result = rdd.reduce(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        System.out.println(result);
    }

    // collect
    private static void collectFunction(JavaSparkContext sc) {
        List<String> list = Lists.newArrayList("aa", "bb", "cc", "dd");
        JavaRDD<String> rdd = sc.parallelize(list);
        List<String> collect = rdd.collect();
        System.out.println(collect); // [aa, bb, cc, dd]
    }

    // take
    private static void takeFunction(JavaSparkContext sc) {
        List<String> list = Lists.newArrayList("aa", "bb", "cc", "dd");
        JavaRDD<String> rdd = sc.parallelize(list);
        List<String> collect = rdd.take(3);
        System.out.println(collect); // [aa, bb, cc]
    }

    // takeSample
    private static void takeSampleFunction(JavaSparkContext sc) {
        List<String> list = Lists.newArrayList("w1","w2","w3","w4","w5");
        JavaRDD<String> listRDD = sc.parallelize(list);
        // 第一个参数：是否可以重复
        // 第二个参数：返回take(n)
        // 第三个参数：代表一个随机数种子，就是抽样算法的初始值
        List<String> result = listRDD.takeSample(false,2,1);
        System.out.println(result);
    }

    // saveAsTextFile
    private static void saveAsTextFileFunction(JavaSparkContext sc) {
        List<String> list = Lists.newArrayList("aa", "bb", "cc", "dd");
        JavaRDD<String> rdd = sc.parallelize(list);
        rdd.saveAsTextFile("/opt/data/output");
    }

    // foreach
    private static void foreachFunction(JavaSparkContext sc) {
        JavaRDD<Integer> javaRDD = sc.parallelize(Arrays.asList(1, 2, 3, 4));
        // 定义Long型的累加器
        LongAccumulator counter = sc.sc().longAccumulator();
        javaRDD.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer v) throws Exception {
                counter.add(1L);
            }
        });
        System.out.println("个数：" + counter.value());
    }

    // count
    private static void countFunction(JavaSparkContext sc) {
        JavaRDD<Integer> javaRDD = sc.parallelize(Arrays.asList(1, 2, 3, 4));
        long count = javaRDD.count();
        System.out.println(count);
    }

    // first
    private static void firstFunction(JavaSparkContext sc) {
        JavaRDD<Integer> javaRDD = sc.parallelize(Arrays.asList(1, 2, 3, 4));
        Integer first = javaRDD.first();
        System.out.println(first);
    }

    // top
    private static void topFunction(JavaSparkContext sc) {
        JavaRDD<Integer> javaRDD = sc.parallelize(Arrays.asList(1, 2, 3, 4));
        List<Integer> top = javaRDD.top(2);
        System.out.println(top);

        JavaRDD<Person> peopleRDD = sc.parallelize(Arrays.asList(
                new Person("Lucy", 10),
                new Person("Tom", 18),
                new Person("Jack", 21),
                new Person("LiLy", 15)
        ));
        List<Person> personTop = peopleRDD.top(2, new SerializableComparator<Person>() {
            @Override
            public int compare(Person o1, Person o2) {
                long age1 = o1.getAge();
                long age2 = o2.getAge();
                if (age1 > age2) {
                    return 1;
                } else if (age1 < age2) {
                    return -1;
                }
                return 0;
            }
        });
        for(Person person : personTop) {
            System.out.println(person);
        }
    }

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("ActionFunctionExample").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        // reduce
        // reduceFunction(sc);
        // collect
        // collectFunction(sc);
        // take
        // takeFunction(sc);
        // takeSample
        // takeSampleFunction(sc);
        // saveAsTextFile
        // saveAsTextFileFunction(sc);
        // foreach
        // foreachFunction(sc);
        // count
        // countFunction(sc);
        // first
        // firstFunction(sc);
        // top
        topFunction(sc);
    }
}
