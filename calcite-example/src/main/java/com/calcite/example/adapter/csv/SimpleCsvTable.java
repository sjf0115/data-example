package com.calcite.example.adapter.csv;

import au.com.bytecode.opencsv.CSVReader;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Source;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * 功能：CsvTable 简单实现
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2023/4/9 上午8:31
 */
public class SimpleCsvTable extends AbstractTable implements ScannableTable {

    private static final Logger LOG = LoggerFactory.getLogger(SimpleCsvTable.class);
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
            rowType = getRowType((JavaTypeFactory) typeFactory, source);
        }
        return rowType;
    }

    // 如何遍历读取CSV文件 全表扫描
    @Override
    public Enumerable<@Nullable Object[]> scan(DataContext root) {
        // 根据 CSV 文件头的字段以及字段类型进行数据转换
        JavaTypeFactory typeFactory = root.getTypeFactory();
        RelDataType rowType = getRowType(typeFactory, source);
        List<RelDataType> fieldTypes = rowType.getFieldList()
                .stream()
                .map(RelDataTypeField::getType)
                .collect(Collectors.toList());

        final AtomicBoolean cancelFlag = DataContext.Variable.CANCEL_FLAG.get(root);
        return new AbstractEnumerable<@Nullable Object[]>() {
            @Override
            public Enumerator<@Nullable Object[]> enumerator() {
                return new SimpleCsvEnumerator<>(source, cancelFlag, fieldTypes);
            }
        };
    }

    // 数据类型转换
    private RelDataType getRowType(JavaTypeFactory typeFactory, Source source) {
        final List<RelDataType> types = new ArrayList<>();
        final List<String> names = new ArrayList<>();
        // 读取 Csv 文件头: 字段以及字段数据类型 格式例如 ID:int,NAME:string
        String[] fields = getCsvDataType(source);
        if (fields == null) {
            fields = new String[]{"EmptyFileHasNoColumns:boolean"};
        }
        for (String field : fields) {
            final String fieldName, fieldType;
            final RelDataType fieldSqlType;
            final int colon = field.indexOf(':');
            if (colon >= 0) {
                // 格式例如 ID:int,NAME:string
                // 字段名称
                fieldName = field.substring(0, colon);
                // 字段类型
                fieldType = field.substring(colon + 1).toLowerCase();
                // Java 数据类型转换为 SQL 数据类型
                switch (fieldType) {
                    case "string":
                        fieldSqlType = toNullableRelDataType(typeFactory, SqlTypeName.VARCHAR);
                        break;
                    case "char":
                        fieldSqlType = toNullableRelDataType(typeFactory, SqlTypeName.CHAR);
                        break;
                    case "int":
                        fieldSqlType = toNullableRelDataType(typeFactory, SqlTypeName.INTEGER);
                        break;
                    case "long":
                        fieldSqlType = toNullableRelDataType(typeFactory, SqlTypeName.BIGINT);
                        break;
                    case "float":
                        fieldSqlType = toNullableRelDataType(typeFactory, SqlTypeName.REAL);
                        break;
                    case "double":
                        fieldSqlType = toNullableRelDataType(typeFactory, SqlTypeName.DOUBLE);
                        break;
                    default:
                        LOG.warn("Found unknown type: {} in file: {} for column: {}. Will assume the type of column is string.",
                                fieldType, source.path(), fieldName);
                        fieldSqlType = toNullableRelDataType(typeFactory, SqlTypeName.VARCHAR);
                        break;
                }
            } else {
                // 没有指定数据类型 格式例如 ID,NAME
                fieldName = field;
                fieldSqlType = typeFactory.createSqlType(SqlTypeName.VARCHAR);
            }
            names.add(fieldName);
            types.add(fieldSqlType);
        }
        if (names.isEmpty()) {
            names.add("line");
            types.add(typeFactory.createSqlType(SqlTypeName.VARCHAR));
        }
        return typeFactory.createStructType(Pair.zip(names, types));
    }

    private static RelDataType toNullableRelDataType(JavaTypeFactory typeFactory, SqlTypeName sqlTypeName) {
        return typeFactory.createTypeWithNullability(typeFactory.createSqlType(sqlTypeName), true);
    }

    // 获取 Csv 文件头中获取字段以及字段类型(需要在Csv文件中自定义)
    private static String[] getCsvDataType(Source source) {
        Objects.requireNonNull(source, "source");
        String[] fields = null;
        try (CSVReader reader = new CSVReader(source.reader())){
            // 读取第一行:字段以及字段数据类型 格式例如 ID:int,NAME:string
            fields = reader.readNext();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return fields;
    }
}
