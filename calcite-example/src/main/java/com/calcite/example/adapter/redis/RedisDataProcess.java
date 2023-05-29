package com.calcite.example.adapter.redis;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import redis.clients.jedis.Jedis;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

/**
 * 功能：RedisDataProcess
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2023/5/29 上午7:41
 */
public class RedisDataProcess {
    String tableName;
    String dataFormat;
    String keyDelimiter;
    RedisDataType dataType;
    RedisDataFormat redisDataFormat;
    List<LinkedHashMap<String, Object>> fields;
    private Jedis jedis;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public RedisDataProcess(Jedis jedis, RedisTableField tableField) {
        this.jedis = jedis;
        String type = jedis.type(tableField.getTableName());
        fields = tableField.getFields();
        dataFormat = tableField.getDataFormat();
        tableName = tableField.getTableName();
        keyDelimiter = tableField.getKeyDelimiter();
        dataType = RedisDataType.fromTypeName(type);
        redisDataFormat = RedisDataFormat.fromTypeName(tableField.getDataFormat());
        objectMapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true)
                .configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true)
                .configure(JsonParser.Feature.ALLOW_COMMENTS, true);
        assert redisDataFormat != null;
        assert dataType != null;
    }

    // 根据数据类型获取值
    public List<Object[]> read() {
        List<Object[]> objs = new ArrayList<>();
        switch (dataType) {
            case STRING:
                return parse(jedis.keys(tableName));
            case LIST:
                return parse(jedis.lrange(tableName, 0, -1));
            case SET:
                return parse(jedis.smembers(tableName));
            case SORTED_SET:
                return parse(jedis.zrange(tableName, 0, -1));
            case HASH:
                return parse(jedis.hvals(tableName));
            default:
                return objs;
        }
    }

    private List<Object[]> parse(Iterable<String> keys) {
        List<Object[]> objs = new ArrayList<>();
        for (String key : keys) {
            if (dataType == RedisDataType.STRING) {
                key = jedis.get(key);
            }
            switch (redisDataFormat) {
                case RAW:
                    objs.add(new Object[]{key});
                    break;
                case JSON:
                    objs.add(parseJson(key));
                    break;
                case CSV:
                    objs.add(parseCsv(key));
                    break;
                default:
                    break;
            }
        }
        return objs;
    }

    public List<Object[]> parse(List<String> keys) {
        List<Object[]> objs = new ArrayList<>();
        for (String key : keys) {
            if (dataType == RedisDataType.STRING) {
                key = jedis.get(key);
            }
            switch (redisDataFormat) {
                case RAW:
                    objs.add(new Object[]{key});
                    break;
                case JSON:
                    objs.add(parseJson(key));
                    break;
                case CSV:
                    objs.add(parseCsv(key));
                    break;
                default:
                    break;
            }
        }
        return objs;
    }

    private Object[] parseJson(String value) {
        assert StringUtils.isNotEmpty(value);
        Object[] arr = new Object[fields.size()];
        try {
            JsonNode jsonNode = objectMapper.readTree(value);
            Object obj;
            for (int i = 0; i < arr.length; i++) {
                obj = fields.get(i).get("mapping");
                if (obj == null) {
                    arr[i] = "";
                } else {
                    arr[i] = jsonNode.findValue(obj.toString());
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Parsing json failed: ", e);
        }
        return arr;
    }

    private Object[] parseCsv(String value) {
        assert StringUtils.isNotEmpty(value);
        String[] values = value.split(keyDelimiter);
        Object[] arr = new Object[fields.size()];
        assert values.length == arr.length;
        for (int i = 0; i < arr.length; i++) {
            arr[i] = values[i] == null ? "" : values[i];
        }
        return arr;
    }
}
