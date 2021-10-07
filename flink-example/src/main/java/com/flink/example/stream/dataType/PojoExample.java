package com.flink.example.stream.dataType;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.MapOperator;

/**
 * 功能：PojO Example
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2021/10/4 上午11:45
 */
public class PojoExample {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Person> personDS = env.fromElements(new Person("Lucy", 18), new Person("Tom", 12));
        MapOperator<Person, String> map = personDS.map(value -> value.getName());
        map.print();
    }
}
