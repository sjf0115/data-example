package com.flink.example.stream.dataType;

import com.flink.example.bean.Person;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.List;
import java.util.Map;

/**
 * 功能：
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/3/19 下午11:41
 */
public class TypesExample {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.fromElements(1, 2, 3)
                .map(i -> Tuple2.of(i, i*i))
                .returns(Types.TUPLE(Types.INT, Types.INT))
                .print();

        // 基本类型
        TypeInformation<Integer> intType = Types.INT;

        // Java 原始类型数组
        TypeInformation<?> primitiveArrayType = Types.PRIMITIVE_ARRAY(Types.INT);
        // Java 对象数组
        TypeInformation<Person[]> objectArrayType = Types.OBJECT_ARRAY(Types.POJO(Person.class));

        // Java POJO
        TypeInformation<Person> pojoType = Types.POJO(Person.class);

//        Types.GENERIC();

        // Java List
        TypeInformation<List<Integer>> listType = Types.LIST(Types.INT);
        // Java Map
        TypeInformation<Map<String, Integer>> mapType = Types.MAP(Types.STRING, Types.INT);
    }
}
