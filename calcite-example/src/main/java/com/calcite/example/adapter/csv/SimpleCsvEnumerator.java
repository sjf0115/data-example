package com.calcite.example.adapter.csv;

import au.com.bytecode.opencsv.CSVReader;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
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

import static org.apache.calcite.linq4j.Nullness.castNonNull;

/**
 * 功能：CsvEnumerator CSV 读取迭代器
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2023/4/9 上午8:34
 */
public class SimpleCsvEnumerator<E> implements Enumerator<E> {

    private static final Logger LOG = LoggerFactory.getLogger(SimpleCsvEnumerator.class);
    // 用于读取 CSV 文件
    private final CSVReader reader;
    private final AtomicBoolean cancelFlag;
    private final AbstractRowConverter<E> rowConverter;
    private @Nullable E current;


    public SimpleCsvEnumerator(Source source, AtomicBoolean cancelFlag, List<RelDataType> fieldTypes, List<Integer> fields) {
        this.cancelFlag = cancelFlag;
        this.rowConverter = (AbstractRowConverter<E>) converter(fieldTypes, fields);
        try {
            this.reader = new CSVReader(source.reader());
            this.reader.readNext();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public E current() {
        return castNonNull(current);
    }

    @Override
    public boolean moveNext() {
        try {
            outer:
            for (;;) {
                if (cancelFlag.get()) {
                    return false;
                }
                // 读取一行记录
                final String[] record = reader.readNext();
                if (record == null) {
                    current = null;
                    reader.close();
                    return false;
                }
                // 数据类型转换
                current = rowConverter.convertRow(record);
                return true;
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void reset() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
        try {
            reader.close();
        } catch (IOException e) {
            throw new RuntimeException("Error closing CSV reader", e);
        }
    }

    //------------------------------------------------------------------------------------------------------------------

    private static AbstractRowConverter<?> converter(List<RelDataType> fieldTypes, List<Integer> fields) {
        return new SimpleRowConverter(fieldTypes, fields);
    }

    // 数据类型转换
    public static RelDataType deduceRowType(JavaTypeFactory typeFactory, Source source, @Nullable List<RelDataType> fieldTypes) {
        final List<RelDataType> types = new ArrayList<>();
        final List<String> names = new ArrayList<>();

        try (CSVReader reader = openCsv(source)) {
            // 读取第一行:字段以及字段数据类型 格式例如 ID:int,NAME:string
            String[] fields = reader.readNext();
            if (fields == null) {
                fields = new String[]{"EmptyFileHasNoColumns:boolean"};
            }
            for (String field : fields) {
                final String fieldName, fieldType;
                final RelDataType fieldRelType;
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
                            fieldRelType = toNullableRelDataType(typeFactory, SqlTypeName.VARCHAR);
                            break;
                        case "char":
                            fieldRelType = toNullableRelDataType(typeFactory, SqlTypeName.CHAR);
                            break;
                        case "int":
                            fieldRelType = toNullableRelDataType(typeFactory, SqlTypeName.INTEGER);
                            break;
                        case "long":
                            fieldRelType = toNullableRelDataType(typeFactory, SqlTypeName.BIGINT);
                            break;
                        case "float":
                            fieldRelType = toNullableRelDataType(typeFactory, SqlTypeName.REAL);
                            break;
                        case "double":
                            fieldRelType = toNullableRelDataType(typeFactory, SqlTypeName.DOUBLE);
                            break;
                        default:
                            LOG.warn("Found unknown type: {} in file: {} for column: {}. Will assume the type of column is string.",
                                    fieldType, source.path(), fieldName);
                            fieldRelType = toNullableRelDataType(typeFactory, SqlTypeName.VARCHAR);
                            break;
                    }
                } else {
                    // 没有指定数据类型 格式例如 ID,NAME
                    fieldName = field;
                    fieldRelType = typeFactory.createSqlType(SqlTypeName.VARCHAR);
                }
                names.add(fieldName);
                types.add(fieldRelType);
                if (fieldTypes != null) {
                    fieldTypes.add(fieldRelType);
                }
            }
        } catch (IOException e) {
            // ignore
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

    static CSVReader openCsv(Source source) throws IOException {
        Objects.requireNonNull(source, "source");
        return new CSVReader(source.reader());
    }
}
