package com.calcite.example.adapter.csv;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Source;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 功能：CsvTable
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2023/4/9 上午8:31
 */
public class SimpleCsvTable extends AbstractTable {
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

    /**
     * 全表扫描
     * @param root
     * @return
     */
    public Enumerable<@Nullable Object[]> scan(DataContext root) {
        JavaTypeFactory typeFactory = root.getTypeFactory();
        // 字段类型
        List<RelDataType> fieldTypes = new ArrayList<>();
        SimpleCsvEnumerator.deduceRowType(typeFactory, source, fieldTypes);
        // 字段？
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
