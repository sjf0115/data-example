package com.clickhouse.example;

import java.sql.ResultSet;

/**
 * 功能：连接示例
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2021/8/1 下午2:35
 */
public class ConnectionExample {
    public static void main(String[] args) {
        String url = "jdbc:clickhouse://106.14.156.67:8123/test";
        ClickHouseProperties properties = new ClickHouseProperties();
// set connection options - see more defined in ClickHouseConnectionSettings
        properties.setClientName("Agent #1");
        ...
// set default request options - more in ClickHouseQueryParam
        properties.setSessionId("default-session-id");
        ...

        ClickHouseDataSource dataSource = new ClickHouseDataSource(url, properties)
        String sql = "select * from mytable";
        Map<ClickHouseQueryParam, String> additionalDBParams = new HashMap<>();
// set request options, which will override the default ones in ClickHouseProperties
        additionalDBParams.put(ClickHouseQueryParam.SESSION_ID, "new-session-id");
        ...
        try (ClickHouseConnection conn = dataSource.getConnection();
             ClickHouseStatement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql, additionalDBParams)) {
            ...
        }
    }
}
