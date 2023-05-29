package com.calcite.example.adapter.redis;

// Redis 支持的数据类型
public enum RedisDataFormat {
    RAW("raw"),
    CSV("csv"),
    JSON("json");

    private final String typeName;

    RedisDataFormat(String typeName) {
        this.typeName = typeName;
    }

    public static RedisDataFormat fromTypeName(String typeName) {
        for (RedisDataFormat type : RedisDataFormat.values()) {
            if (type.getTypeName().equals(typeName)) {
                return type;
            }
        }
        return null;
    }

    public String getTypeName() {
        return this.typeName;
    }
}
