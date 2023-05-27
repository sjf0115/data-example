package com.calcite.example.adapter.csv;

import org.apache.calcite.rel.type.RelDataType;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

/**
 * 功能：SimpleRowConverter 行转换器
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2023/4/9 下午11:30
 */
public class SimpleRowConverter extends AbstractRowConverter<@Nullable Object[]> {
    private final List<RelDataType> fieldTypes;

    public SimpleRowConverter(List<RelDataType> fieldTypes) {
        this.fieldTypes = fieldTypes;
    }

    @Override
    public @Nullable Object[] convertRow(@Nullable String[] rows) {
        final @Nullable Object[] objects = new Object[fieldTypes.size()];
        for (int i = 0; i < fieldTypes.size(); i++) {
            objects[i] = convert(fieldTypes.get(i), rows[i]);
        }
        return objects;
    }
}
