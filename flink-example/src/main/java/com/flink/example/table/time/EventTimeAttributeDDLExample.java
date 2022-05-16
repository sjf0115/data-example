package com.flink.example.table.time;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 功能：事件时间属性 DDL 方式定义
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/5/4 下午6:50
 */
public class EventTimeAttributeDDLExample {
    public static void main(String[] args) throws Exception {
        // TableEnvironment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 创建输入表
        tEnv.executeSql("CREATE TABLE user_behavior (\n" +
                "  uid BIGINT COMMENT '用户Id',\n" +
                "  pid BIGINT COMMENT '商品Id',\n" +
                "  cid BIGINT COMMENT '商品类目Id',\n" +
                "  type STRING COMMENT '行为类型',\n" +
                "  ts BIGINT COMMENT '行为时间', -- 时间戳 Long 型 1618989564564\n" +
                "  ts_ltz AS TO_TIMESTAMP_LTZ(ts, 3),\n" +
                "  WATERMARK FOR ts_ltz AS ts_ltz - INTERVAL '1' MINUTE -- 在 ts_ltz 上定义watermark，ts_ltz 成为事件时间列\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'user_behavior',\n" +
                "  'properties.bootstrap.servers' = 'localhost:9092',\n" +
                "  'properties.group.id' = 'user_behavior_timestamp',\n" +
                "  'scan.startup.mode' = 'latest-offset',\n" +
                "  'format' = 'json',\n" +
                "  'json.ignore-parse-errors' = 'true',\n" +
                "  'json.fail-on-missing-field' = 'false'\n" +
                ")");

        // 创建输出表
        tEnv.executeSql("CREATE TABLE user_behavior_cnt (\n" +
                "  row_time TIMESTAMP(3) COMMENT '窗口开始时间',\n" +
                "  window_start TIMESTAMP(3) COMMENT '窗口开始时间',\n" +
                "  window_end TIMESTAMP(3) COMMENT '窗口结束时间',\n" +
                "  cnt BIGINT COMMENT '次数'\n" +
                ") WITH (\n" +
                "  'connector' = 'print',\n" +
                "  'print-identifier' = 'ET',\n" +
                "  'sink.parallelism' = '1'\n" +
                ")");

        // 执行计算并输出
        String sql = "INSERT INTO user_behavior_cnt\n" +
                "SELECT\n" +
                "  TUMBLE_ROWTIME(ts_ltz, INTERVAL '1' HOUR) AS row_time,\n" +
                "  TUMBLE_START(ts_ltz, INTERVAL '1' HOUR) AS window_start,\n" +
                "  TUMBLE_END(ts_ltz, INTERVAL '1' HOUR) AS window_end,\n" +
                "  COUNT(*) AS cnt\n" +
                "FROM user_behavior\n" +
                "GROUP BY TUMBLE(ts_ltz, INTERVAL '1' HOUR)";
        tEnv.executeSql(sql);

        // 执行
        //env.execute();
    }
}
