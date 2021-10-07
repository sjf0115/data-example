package com.flink.example.stream.dataType;

import org.apache.flink.api.common.typeinfo.TypeInfoFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.lang.reflect.Type;
import java.util.Map;

/**
 * 功能：自定义 TypeInfoFactory
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2021/10/4 下午12:17
 */
public class CustomTypeInfoFactory extends TypeInfoFactory<CustomTuple> {
    @Override
    public TypeInformation<CustomTuple> createTypeInfo(Type t, Map<String, TypeInformation<?>> genericParameters) {
        return new CustomTupleTypeInfo(genericParameters.get("T0"), genericParameters.get("T1"));
    }
}
