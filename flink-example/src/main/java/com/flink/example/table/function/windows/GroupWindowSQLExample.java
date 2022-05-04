package com.flink.example.table.function.windows;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * 功能：Group Windows SQL 示例
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/5/3 下午9:29
 */
public class GroupWindowSQLExample {
    public static void main(String[] args) throws Exception {
        // TableEnvironment
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        // 创建输入表
        String sourceSql = "CREATE TABLE user_behavior (\n" +
                "  uid BIGINT COMMENT '用户Id',\n" +
                "  pid BIGINT COMMENT '商品Id',\n" +
                "  cid BIGINT COMMENT '商品类目Id',\n" +
                "  type STRING COMMENT '行为类型',\n" +
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

        // 创建输出表
        String sinkSql = "CREATE TABLE user_behavior_result (\n" +
                "  uid BIGINT COMMENT '用户Id',\n" +
                "  window_tart STRING COMMENT '窗口开始时间',\n" +
                "  window_end STRING COMMENT '窗口结束时间',\n" +
                "  num BIGINT COMMENT '条数'\n" +
                ") WITH (\n" +
                "  'connector' = 'print',\n" +
                "  'print-identifier' = 'behavior',\n" +
                "  'sink.parallelism' = '1'\n" +
                ")";
        tEnv.executeSql(sinkSql);

        // 执行计算并输出
        String sql = "INSERT INTO user_behavior_result\n" +
                "SELECT\n" +
                "  uid,\n" +
                "  DATE_FORMAT(TUMBLE_START(ts_ltz, INTERVAL '1' MINUTE), 'yyyy-MM-dd HH:mm:ss') AS window_tart,\n" +
                "  DATE_FORMAT(TUMBLE_END(ts_ltz, INTERVAL '1' MINUTE), 'yyyy-MM-dd HH:mm:ss') AS window_end,\n" +
                "  COUNT(*) AS num\n" +
                "FROM user_behavior\n" +
                "GROUP BY TUMBLE(ts_ltz, INTERVAL '1' MINUTE), uid";
        tEnv.executeSql(sql);
    }
}
