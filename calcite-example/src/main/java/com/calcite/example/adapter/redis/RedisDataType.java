package com.calcite.example.adapter.redis;

// 支持的 Redis 数据类型
public enum RedisDataType {
    STRING("string"),
    HASH("hash"),
    LIST("list"),
    SET("set"),
    SORTED_SET("zset"),
    HYPER_LOG_LOG("pfadd"),
    PUBSUB("publish");

    private final String typeName;

    RedisDataType(String typeName) {
        this.typeName = typeName;
    }

    public static RedisDataType fromTypeName(String typeName) {
        for (RedisDataType type : RedisDataType.values()) {
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
