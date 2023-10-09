package com.spark.example.core.base;

import com.google.common.collect.Lists;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.util.LongAccumulator;

import java.util.List;

/**
 * 功能：累加器陷阱
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2023/10/9 08:50
 */
public class AccumulatorTrap {
    public static void main(String[] args) {
        String appName = "AccumulatorTrap";
        SparkConf conf = new SparkConf().setAppName(appName).setMaster("local[*]");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);

        LongAccumulator evenAccumulator = sparkContext.sc().longAccumulator("Even Num Accumulator");
        LongAccumulator oddAccumulator = sparkContext.sc().longAccumulator("Odd Num Accumulator");

        List<Integer> numList = Lists.newArrayList();
        for(int i = 0;i < 10;i++){
            numList.add(i);
        }

        JavaRDD<Integer> resultRDD = sparkContext.parallelize(numList)
                // transform
                .map(new Function<Integer, Integer>() {
                    @Override
                    public Integer call(Integer num) throws Exception {
                        if (num % 2 == 0) {
                            evenAccumulator.add(1L);
                            return 0;
                        } else {
                            oddAccumulator.add(1L);
                            return 1;
                        }
                    }
                });

        // the first action
        resultRDD.count();
        System.out.println("Odd Num Count : " + oddAccumulator.value());
        System.out.println("Even Num Count : " + evenAccumulator.value());

        // the second action
        resultRDD.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer num) throws Exception {
                System.out.println(num);
            }
        });
        System.out.println("Odd Num Count : " + oddAccumulator.value());
        System.out.println("Even Num Count : " + evenAccumulator.value());
    }
}
