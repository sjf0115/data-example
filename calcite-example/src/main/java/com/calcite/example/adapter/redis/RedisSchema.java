package com.calcite.example.adapter.redis;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import org.apache.calcite.model.JsonCustomTable;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.commons.lang3.StringUtils;

import java.util.*;
import java.util.stream.Collectors;

/**
 * 功能：RedisSchema
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2023/5/28 下午11:49
 */
public class RedisSchema extends AbstractSchema {
    public final String host;
    public final int port;
    public final int database;
    public final String password;
    public final List<Map<String, Object>> tables;
    private Map<String, Table> tableMap = null;

    RedisSchema(String host, int port, int database, String password, List<Map<String, Object>> tables) {
        this.host = host;
        this.port = port;
        this.database = database;
        this.password = password;
        this.tables = tables;
    }

    @Override
    protected Map<String, Table> getTableMap() {
        // 自定义表
        JsonCustomTable[] jsonCustomTables = new JsonCustomTable[tables.size()];
        // 自定义表的名称
        Set<String> tableNames = Arrays.stream(tables.toArray(jsonCustomTables))
                .map(e -> e.name)
                .collect(Collectors.toSet());
        // 表定义表名称与实体的映射
        tableMap = Maps.asMap(
                ImmutableSet.copyOf(tableNames),
                // 缓存
                CacheBuilder.newBuilder().build(CacheLoader.from(this::createTable))
        );
        return tableMap;
    }

    // 创建 Table 指定表名
    private Table createTable(String tableName) {
        RedisConfig redisConfig = new RedisConfig(host, port, database, password);
        return RedisTable.of(RedisSchema.this, tableName, redisConfig, null);
    }

    public RedisTableField getTableField(String tableName) {
        RedisTableField tableFieldInfo = new RedisTableField();
        List<LinkedHashMap<String, Object>> fields = new ArrayList<>();
        Map<String, Object> map;
        String dataFormat = "";
        String keyDelimiter = "";
        for (int i = 0; i < this.tables.size(); i++) {
            JsonCustomTable jsonCustomTable = (JsonCustomTable) this.tables.get(i);
            if (jsonCustomTable.name.equals(tableName)) {
                map = jsonCustomTable.operand;
                if (map.get("dataFormat") == null) {
                    throw new RuntimeException("dataFormat is null");
                }
                if (map.get("fields") == null) {
                    throw new RuntimeException("fields is null");
                }
                dataFormat = map.get("dataFormat").toString();
                fields = (List<LinkedHashMap<String, Object>>) map.get("fields");
                if (map.get("keyDelimiter") != null) {
                    keyDelimiter = map.get("keyDelimiter").toString();
                }
                break;
            }
        }
        tableFieldInfo.setTableName(tableName);
        tableFieldInfo.setDataFormat(dataFormat);
        tableFieldInfo.setFields(fields);
        if (StringUtils.isNotEmpty(keyDelimiter)) {
            tableFieldInfo.setKeyDelimiter(keyDelimiter);
        }
        return tableFieldInfo;
    }
}
