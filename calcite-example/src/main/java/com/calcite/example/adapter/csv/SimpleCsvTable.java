package com.calcite.example.adapter.csv;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Source;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 功能：CsvTable 简单实现
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2023/4/9 上午8:31
 */
public class SimpleCsvTable extends AbstractTable implements ScannableTable {
    protected final Source source;
    protected final @Nullable RelProtoDataType protoRowType;
    private @Nullable RelDataType rowType;

    SimpleCsvTable(Source source) {
        this.source = source;
        this.protoRowType = null;
    }

    SimpleCsvTable(Source source, @Nullable RelProtoDataType protoRowType) {
        this.source = source;
        this.protoRowType = protoRowType;
    }

    @Override
    public String toString() {
        return "SimpleCsvTable";
    }

    // 定义Table的字段以及字段类型
    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        if (protoRowType != null) {
            return protoRowType.apply(typeFactory);
        }
        if (rowType == null) {
            rowType = SimpleCsvEnumerator.deduceRowType((JavaTypeFactory) typeFactory, source, null);
        }
        return rowType;
    }

    // 如何遍历读取CSV文件 全表扫描
    @Override
    public Enumerable<@Nullable Object[]> scan(DataContext root) {
        // 根据 CSV 文件中第一行的字段以及字段类型进行数据转换
        JavaTypeFactory typeFactory = root.getTypeFactory();
        List<RelDataType> fieldTypes = new ArrayList<>();
        SimpleCsvEnumerator.deduceRowType(typeFactory, source, fieldTypes);

        List<Integer> fields = ImmutableIntList.identity(fieldTypes.size());

        final AtomicBoolean cancelFlag = DataContext.Variable.CANCEL_FLAG.get(root);
        return new AbstractEnumerable<@Nullable Object[]>() {
            @Override
            public Enumerator<@Nullable Object[]> enumerator() {
                return new SimpleCsvEnumerator<>(source, cancelFlag, fieldTypes, fields);
            }
        };
    }
}
