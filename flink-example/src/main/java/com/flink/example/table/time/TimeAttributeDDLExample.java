package com.flink.example.table.time;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * 功能：时间属性 DDL 方式定义
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/5/4 下午6:50
 */
public class TimeAttributeDDLExample {
    public static void main(String[] args) throws Exception {
        // TableEnvironment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 创建输入表
        String sourceSql = "CREATE TABLE user_behavior (\n" +
                "  uid BIGINT COMMENT '用户Id',\n" +
                "  ts BIGINT COMMENT '行为时间',\n" +
                "  ts_ltz AS TO_TIMESTAMP_LTZ(ts, 3),\n" +
                "  proctime AS PROCTIME(), -- 通过计算列产生一个处理时间列\n" +
                "  WATERMARK FOR ts_ltz AS ts_ltz - INTERVAL '1' MINUTE -- 在 ts_ltz 上定义watermark，ts_ltz 成为事件时间列\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'user_behavior',\n" +
                "  'properties.bootstrap.servers' = 'localhost:9092',\n" +
                "  'properties.group.id' = 'kafka-connector-value',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'json',\n" +
                "  'json.ignore-parse-errors' = 'false',\n" +
                "  'json.fail-on-missing-field' = 'true'\n" +
                ")";
        tEnv.executeSql(sourceSql);

        // 执行计算并输出
        String sql = "SELECT * FROM user_behavior";
        Table table = tEnv.sqlQuery(sql);
        DataStream<Row> stream = tEnv.toAppendStream(table, Row.class);
        stream.print();

        // 执行
        env.execute();
    }
}
