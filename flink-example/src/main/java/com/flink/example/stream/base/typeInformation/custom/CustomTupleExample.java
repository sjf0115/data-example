package com.flink.example.stream.base.typeInformation.custom;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.util.Map;

/**
 * 功能：CustomTupleExample
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2021/10/4 下午7:18
 */
public class CustomTupleExample {
    public static void main(String[] args) {
        CustomTuple<String, Integer> customTuple = new CustomTuple<>("Lucy", 18);
        System.out.println(customTuple);

        TypeHint<CustomTuple<String, Integer>> hint = new TypeHint<CustomTuple<String, Integer>>() {
            @Override
            public TypeInformation<CustomTuple<String, Integer>> getTypeInfo() {
                return super.getTypeInfo();
            }
        };

        TypeInformation<CustomTuple<String, Integer>> typeInfo = hint.getTypeInfo();
        Map<String, TypeInformation<?>> parameters = typeInfo.getGenericParameters();
        for (String key : parameters.keySet()) {
            System.out.println("Key: " + key + ", Value: " + parameters.get(key));
        }
    }
}
