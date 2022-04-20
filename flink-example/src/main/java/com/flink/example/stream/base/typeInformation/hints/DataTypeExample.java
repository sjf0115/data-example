package com.flink.example.stream.base.typeInformation.hints;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

import java.util.ArrayList;

/**
 * 功能：数据类型示例
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/4/16 下午10:28
 */
public class DataTypeExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 基本类型
        // 创建Integer类型的数据集
        DataStream<Integer> integerElements = env.fromElements(1, 2, 3);
        integerElements.print();
        // 创建String类型的数据集
        DataStream<String> stringElements = env.fromElements("1", "2", "3");
        stringElements.print();

        // 数组类型
        int[] a = {1, 2};
        int[] b = {3, 4};
        DataStream<int[]> arrayElements = env.fromElements(a, b);
        arrayElements.print();

        // Tuple 类型
        DataStream<Tuple2> tupleElements = env.fromElements(new Tuple2(1, "a"), new Tuple2(2, "b"));
        tupleElements.print();

        // Row 类型
        DataStream<Row> rowElements = env.fromElements(Row.of(0, "a", 3.14));
        rowElements.print();

        // List 类型
        DataStream<ArrayList<Integer>> listElements = env.fromElements(
                Lists.newArrayList(1, 2), Lists.newArrayList(3, 4)
        );
        listElements.print();
        env.execute();
    }
}
