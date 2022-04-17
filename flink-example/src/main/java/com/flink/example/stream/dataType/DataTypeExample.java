package com.flink.example.stream.dataType;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

import java.util.Arrays;

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
        // 创建String类型的数据集
        DataStream<String> stringElements = env.fromElements("1", "2", "3");


        DataStream<Integer> list = env.fromCollection(Arrays.asList(1, 2, 3));

        // Tuple类型
        DataStream<Tuple2> tupleElements = env.fromElements(new Tuple2(1, "a"), new Tuple2(2, "b"));


        Row row = new Row(3);
        row.setField(0, 1);
        row.setField(1, "a");
        DataStream<Row> rowElements = env.fromElements(row);
        rowElements.print();

        env.execute();
    }
}
