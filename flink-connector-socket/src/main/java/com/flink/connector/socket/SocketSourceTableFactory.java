package com.flink.connector.socket;

import org.apache.flink.table.factories.StreamTableSourceFactory;
import org.apache.flink.table.sources.StreamTableSource;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 功能：Socket Source TableFactory
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/5/26 下午10:40
 */
public class SocketSourceTableFactory implements StreamTableSourceFactory {

    public Map<String, String> requiredContext() {
        Map<String, String> context = new HashMap<>();
        context.put("update-mode", "append");
        context.put("connector.type", "socket");
        return context;
    }

    public List<String> supportedProperties() {
        List<String> properties = new ArrayList<>();
        properties.add("hostname");
        properties.add("port");
        return properties;
    }

    public StreamTableSource createStreamTableSource(Map properties) {
        return null;
    }
}
