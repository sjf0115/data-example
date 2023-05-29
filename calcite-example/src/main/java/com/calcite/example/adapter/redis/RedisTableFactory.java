package com.calcite.example.adapter.redis;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.TableFactory;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Map;

/**
 * 功能：RedisTableFactory
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2023/5/29 下午11:44
 */
public class RedisTableFactory implements TableFactory {
    public static final RedisTableFactory INSTANCE = new RedisTableFactory();

    private RedisTableFactory() {

    }

    @Override public Table create(SchemaPlus schema, String tableName, Map operand, @Nullable RelDataType rowType) {
        final RedisSchema redisSchema = schema.unwrap(RedisSchema.class);
        final RelProtoDataType protoRowType = rowType != null ? RelDataTypeImpl.proto(rowType) : null;
        return RedisTable.of(redisSchema, tableName, operand, protoRowType);
    }
}
