package com.calcite.example.adapter.memory;

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.impl.AbstractTable;

/**
 * 功能：
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2024/11/24 16:12
 */
public class MemoryTable  extends AbstractTable implements ScannableTable {
    @Override
    public Enumerable<Object[]> scan(DataContext root) {
        return null;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return null;
    }
}
