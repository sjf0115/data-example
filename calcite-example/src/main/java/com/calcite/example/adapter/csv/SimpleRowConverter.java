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
    private final List<Integer> fields;

    public SimpleRowConverter(List<RelDataType> fieldTypes, List<Integer> fields) {
        this.fieldTypes = fieldTypes;
        this.fields = fields;
    }

    @Override
    public @Nullable Object[] convertRow(@Nullable String[] rows) {
        final @Nullable Object[] objects = new Object[fields.size()];
        for (int i = 0; i < fields.size(); i++) {
            int field = fields.get(i);
            objects[i] = convert(fieldTypes.get(field), rows[field]);
        }
        return objects;
    }
}
