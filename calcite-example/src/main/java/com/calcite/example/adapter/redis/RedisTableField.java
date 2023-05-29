package com.calcite.example.adapter.redis;

import java.util.LinkedHashMap;
import java.util.List;

/**
 * 功能：RedisTableField
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2023/5/29 上午7:36
 */
public class RedisTableField {
    private String tableName;
    private String dataFormat;
    private List<LinkedHashMap<String, Object>> fields;
    private String keyDelimiter = ":";

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getDataFormat() {
        return dataFormat;
    }

    public void setDataFormat(String dataFormat) {
        this.dataFormat = dataFormat;
    }

    public List<LinkedHashMap<String, Object>> getFields() {
        return fields;
    }

    public void setFields(List<LinkedHashMap<String, Object>> fields) {
        this.fields = fields;
    }

    public String getKeyDelimiter() {
        return keyDelimiter;
    }

    public void setKeyDelimiter(String keyDelimiter) {
        this.keyDelimiter = keyDelimiter;
    }

    @Override
    public String toString() {
        return "RedisTableField{" +
                "tableName='" + tableName + '\'' +
                ", dataFormat='" + dataFormat + '\'' +
                ", fields=" + fields +
                ", keyDelimiter='" + keyDelimiter + '\'' +
                '}';
    }
}
