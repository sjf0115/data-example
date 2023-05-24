package com.calcite.example.adapter.csv;

import org.apache.calcite.rel.type.RelDataType;
import org.checkerframework.checker.nullness.qual.Nullable;


/**
 * 功能：行转换器-简单实现 Java 数据类型转换为目标数据类型
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2023/4/9 下午10:02
 */
public abstract class AbstractRowConverter<E> {
    abstract E convertRow(@Nullable String[] rows);

    protected @Nullable Object convert(@Nullable RelDataType fieldType, @Nullable String string) {
        if (fieldType == null || string == null) {
            return string;
        }
        switch (fieldType.getSqlTypeName()) {
            case BOOLEAN:
                if (string.length() == 0) {
                    return null;
                }
                return Boolean.parseBoolean(string);
            case INTEGER:
                if (string.length() == 0) {
                    return null;
                }
                return Integer.parseInt(string);
            case BIGINT:
                if (string.length() == 0) {
                    return null;
                }
                return Long.parseLong(string);
            case FLOAT:
                if (string.length() == 0) {
                    return null;
                }
                return Float.parseFloat(string);
            case DOUBLE:
                if (string.length() == 0) {
                    return null;
                }
                return Double.parseDouble(string);
            case VARCHAR:
            default:
                return string;
        }
    }
}
