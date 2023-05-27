package com.calcite.example.adapter.csv;

import au.com.bytecode.opencsv.CSVReader;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.Source;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
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


    public SimpleCsvEnumerator(Source source, AtomicBoolean cancelFlag, List<RelDataType> fieldTypes) {
        this.cancelFlag = cancelFlag;
        this.rowConverter = (AbstractRowConverter<E>) converter(fieldTypes);
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
                final String[] row = reader.readNext();
                if (row == null) {
                    current = null;
                    reader.close();
                    return false;
                }
                // 读取出来的都是 String 需要转换为对应的数据类型
                current = rowConverter.convertRow(row);
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

    private static AbstractRowConverter<?> converter(List<RelDataType> fieldTypes) {
        return new SimpleRowConverter(fieldTypes);
    }
}
