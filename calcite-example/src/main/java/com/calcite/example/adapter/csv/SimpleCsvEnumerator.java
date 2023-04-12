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
 * 功能：CsvEnumerator
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2023/4/9 上午8:34
 */
public class SimpleCsvEnumerator<E> implements Enumerator<E> {

    private static final Logger LOG = LoggerFactory.getLogger(SimpleCsvEnumerator.class);

    private final CSVReader reader;
    private final @Nullable List<@Nullable String> filterValues;
    private final AtomicBoolean cancelFlag;
    private final AbstractRowConverter<E> rowConverter;
    private @Nullable E current;


    public SimpleCsvEnumerator(Source source, AtomicBoolean cancelFlag, List<RelDataType> fieldTypes, List<Integer> fields) {
        this.cancelFlag = cancelFlag;
        this.filterValues = null;
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
                final String[] strings = reader.readNext();
                if (strings == null) {
                    current = null;
                    reader.close();
                    return false;
                }
                if (filterValues != null) {
                    for (int i = 0; i < strings.length; i++) {
                        String filterValue = filterValues.get(i);
                        if (filterValue != null) {
                            if (!filterValue.equals(strings[i])) {
                                continue outer;
                            }
                        }
                    }
                }
                current = rowConverter.convertRow(strings);
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
            String[] strings = reader.readNext();
            if (strings == null) {
                strings = new String[]{"EmptyFileHasNoColumns:boolean"};
            }
            for (String string : strings) {
                final String name;
                final RelDataType fieldType;
                final int colon = string.indexOf(':');
                if (colon >= 0) {
                    name = string.substring(0, colon);
                    String typeString = string.substring(colon + 1);
                    switch (typeString) {
                        case "string":
                            fieldType = toNullableRelDataType(typeFactory, SqlTypeName.VARCHAR);
                            break;
                        case "char":
                            fieldType = toNullableRelDataType(typeFactory, SqlTypeName.CHAR);
                            break;
                        case "int":
                            fieldType = toNullableRelDataType(typeFactory, SqlTypeName.INTEGER);
                            break;
                        case "long":
                            fieldType = toNullableRelDataType(typeFactory, SqlTypeName.BIGINT);
                            break;
                        case "float":
                            fieldType = toNullableRelDataType(typeFactory, SqlTypeName.REAL);
                            break;
                        case "double":
                            fieldType = toNullableRelDataType(typeFactory, SqlTypeName.DOUBLE);
                            break;
                        default:
                            LOG.warn("Found unknown type: {} in file: {} for column: {}. Will assume the type of "
                                            + "column is string.",
                                    typeString, source.path(), name);
                            fieldType = toNullableRelDataType(typeFactory, SqlTypeName.VARCHAR);
                            break;
                    }
                } else {
                    name = string;
                    fieldType = typeFactory.createSqlType(SqlTypeName.VARCHAR);
                }
                names.add(name);
                types.add(fieldType);
                if (fieldTypes != null) {
                    fieldTypes.add(fieldType);
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
