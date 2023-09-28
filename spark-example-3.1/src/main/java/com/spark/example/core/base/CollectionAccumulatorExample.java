package com.spark.example.core.base;

import com.google.common.collect.Lists;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.util.AccumulatorV2;

import java.util.ArrayList;
import java.util.List;

/**
 * 功能：自定义累加器 CollectionAccumulator
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2023/9/28 08:16
 */
public class CollectionAccumulatorExample {
    private static class CollectionAccumulator<T> extends AccumulatorV2<T, List<T>> {

        private List<T> collection = Lists.newArrayList();

        @Override
        public boolean isZero() {
            return collection.isEmpty();
        }

        @Override
        public AccumulatorV2<T, List<T>> copy() {
            CollectionAccumulator<T> accumulator = new CollectionAccumulator<>();
            synchronized (accumulator) {
                accumulator.collection.addAll(collection);
            }
            return accumulator;
        }

        @Override
        public void reset() {
            collection.clear();
        }

        @Override
        public void add(T v) {
            collection.add(v);
        }

        @Override
        public void merge(AccumulatorV2<T, List<T>> other) {
            if(other instanceof CollectionAccumulator){
                collection.addAll(((CollectionAccumulator) other).collection);
            }
            else {
                throw new UnsupportedOperationException("Cannot merge " + this.getClass().getName() + " with " + other.getClass().getName());
            }
        }

        @Override
        public List<T> value() {
            return new ArrayList<>(collection);
        }
    }

    public static void main(String[] args) {
        String appName = "CustomAccumulatorExample";
        SparkConf conf = new SparkConf().setAppName(appName).setMaster("local[*]");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);

        List<String> list = Lists.newArrayList();
        list.add("27.34832,111.32135");
        list.add("34.88478,185.17841");
        list.add("39.92378,119.50802");
        list.add("94,119.50802");

        // 创建累加器
        CollectionAccumulator<String> collectionAccumulator = new CollectionAccumulator<>();
        // 注册累加器
        sparkContext.sc().register(collectionAccumulator, "Illegal Coordinates");
        // 过滤非法坐标
        List<String> collect = sparkContext.parallelize(list)
                .filter(new Function<String, Boolean>() {
                    @Override
                    public Boolean call(String str) throws Exception {
                        String[] coordinate = str.split(",");
                        double lat = Double.parseDouble(coordinate[0]);
                        double lon = Double.parseDouble(coordinate[1]);
                        if (Math.abs(lat) > 90 || Math.abs(lon) > 180) {
                            collectionAccumulator.add(str);
                            return true;
                        }
                        return false;
                    }
                }).collect();

        System.out.println("非法坐标采集：" + collect.toString());
        System.out.println("非法坐标累加器记录" + collectionAccumulator.value());
    }
}
