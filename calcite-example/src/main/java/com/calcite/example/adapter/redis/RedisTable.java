package com.calcite.example.adapter.redis;

import com.google.common.collect.ImmutableMap;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.util.Pair;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 功能：RedisTable
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2023/5/28 下午11:58
 */
public class RedisTable extends AbstractTable implements ScannableTable {
    final RedisSchema schema;
    final String tableName;
    final RelProtoDataType protoRowType;
    final ImmutableMap<String, Object> allFields;
    final String dataFormat;
    final RedisConfig redisConfig;

    public RedisTable(RedisSchema schema, String tableName, RelProtoDataType protoRowType,
            Map<String, Object> allFields, String dataFormat, RedisConfig redisConfig) {
        this.schema = schema;
        this.tableName = tableName;
        this.protoRowType = protoRowType;
        this.allFields = allFields == null ? ImmutableMap.of() : ImmutableMap.copyOf(allFields);
        this.dataFormat = dataFormat;
        this.redisConfig = redisConfig;
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root) {
        return new AbstractEnumerable<Object[]>() {
            @Override public Enumerator<Object[]> enumerator() {
                return new RedisEnumerator(redisConfig, schema, tableName);
            }
        };
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        if (protoRowType != null) {
            return protoRowType.apply(typeFactory);
        }
        final List<RelDataType> types = new ArrayList<RelDataType>(allFields.size());
        final List<String> names = new ArrayList<String>(allFields.size());

        for (Object key : allFields.keySet()) {
            final RelDataType type = typeFactory.createJavaType(allFields.get(key).getClass());
            names.add(key.toString());
            types.add(type);
        }
        return typeFactory.createStructType(Pair.zip(names, types));
    }

    // 创建 Table 实例
    static Table of(RedisSchema schema, String tableName, RedisConfig redisConfig, RelProtoDataType protoRowType) {
        RedisTableField tableField = schema.getTableField(tableName);
        String dataFormat = tableField.getDataFormat();
        Map<String, Object> allFields = deduceRowType(tableField);
        return new RedisTable(schema, tableName, protoRowType, allFields, dataFormat, redisConfig);
    }

    static Table of(RedisSchema schema, String tableName, Map operand, RelProtoDataType protoRowType) {
        RedisConfig redisConfig = new RedisConfig(schema.host, schema.port, schema.database, schema.password);
        return of(schema, tableName, redisConfig, protoRowType);
    }

    static Map<String, Object> deduceRowType(RedisTableField tableField) {
        final Map<String, Object> fieldBuilder = new LinkedHashMap<>();
        // 支持的数据格式
        String dataFormat = tableField.getDataFormat();
        RedisDataFormat redisDataFormat = RedisDataFormat.fromTypeName(dataFormat);
        assert redisDataFormat != null;
        if (redisDataFormat == RedisDataFormat.RAW) {
            fieldBuilder.put("key", "key");
        } else {
            for (LinkedHashMap<String, Object> field : tableField.getFields()) {
                fieldBuilder.put(field.get("name").toString(), field.get("type").toString());
            }
        }
        return fieldBuilder;
    }
}
