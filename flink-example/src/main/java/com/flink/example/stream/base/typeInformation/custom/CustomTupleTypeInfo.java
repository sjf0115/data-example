package com.flink.example.stream.base.typeInformation.custom;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;

import java.util.Map;

/**
 * 功能：CustomTupleTypeInfo
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2021/10/4 下午6:52
 */
public class CustomTupleTypeInfo<T0, T1> extends TypeInformation<CustomTuple<T0, T1>> {
    private TypeInformation field0;
    private TypeInformation field1;

    public CustomTupleTypeInfo(TypeInformation field0, TypeInformation field1) {
        this.field0 = field0;
        this.field1 = field1;
    }

    public TypeInformation getField0() {
        return field0;
    }

    public TypeInformation getField1() {
        return field1;
    }

    @Override
    public boolean isBasicType() {
        return false;
    }

    @Override
    public boolean isTupleType() {
        return false;
    }

    @Override
    public int getArity() {
        return 0;
    }

    @Override
    public int getTotalFields() {
        return 0;
    }

    @Override
    public Class<CustomTuple<T0, T1>> getTypeClass() {
        return null;
    }

    @Override
    public boolean isKeyType() {
        return false;
    }

    @Override
    public TypeSerializer<CustomTuple<T0, T1>> createSerializer(ExecutionConfig config) {
        return null;
    }

    @Override
    public String toString() {
        return null;
    }

    @Override
    public boolean equals(Object obj) {
        return false;
    }

    @Override
    public int hashCode() {
        return 0;
    }

    @Override
    public boolean canEqual(Object obj) {
        return false;
    }

    @Override
    public Map<String, TypeInformation<?>> getGenericParameters() {
        return super.getGenericParameters();
    }
}
