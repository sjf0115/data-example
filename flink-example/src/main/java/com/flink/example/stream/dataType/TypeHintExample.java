package com.flink.example.stream.dataType;

import com.flink.example.bean.Person;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * 功能：TypeHint Example
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2021/10/4 下午12:07
 */
public class TypeHintExample {
    public static void main(String[] args) {
        // 非泛型
        TypeInformation<Person> personTypeInfo = TypeInformation.of(Person.class);
        Class<Person> typeClass = personTypeInfo.getTypeClass();
        System.out.println(typeClass);

        // 泛型 方法1
        TypeInformation<Tuple2<String, String>> tupleTypeInfo = TypeInformation.of(new TypeHint<Tuple2<String, String>>() {
        });
        Class<Tuple2<String, String>> tupleClass = tupleTypeInfo.getTypeClass();
        System.out.println(tupleClass);

        // 泛型 方法2
        TypeHint<Tuple2<String, Long>> tupleTypeHint = new TypeHint<Tuple2<String, Long>>() {
        };
        TypeInformation<Tuple2<String, Long>> tupleClass2 = tupleTypeHint.getTypeInfo();
        System.out.println(tupleClass2);
    }
}
