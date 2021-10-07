package com.flink.example.stream.dataType;

import org.apache.flink.api.common.typeinfo.TypeInfo;

/**
 * 功能：自定义 TypeInformation
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2021/10/4 下午12:16
 */
@TypeInfo(CustomTypeInfoFactory.class)
public class CustomTuple<T0, T1> {
    public T0 f0;
    public T1 f1;

    public CustomTuple(T0 f0, T1 f1) {
        this.f0 = f0;
        this.f1 = f1;
    }

    @Override
    public String toString() {
        return "CustomTuple{" +
                "f0=" + f0 +
                ", f1=" + f1 +
                '}';
    }
}
