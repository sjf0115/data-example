package com.flink.example.table.function.windows;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 功能：Group Windows SQL 示例
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/5/3 下午9:29
 */
public class GroupWindowSQLExample {

    private static final Logger LOG = LoggerFactory.getLogger(GroupWindowSQLExample.class);

    public static void main(String[] args) throws Exception {
        // TableEnvironment
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        LOG.info("create source table");

        // 1. 示例1 基于事件时间的滚动窗口
        runEventTimeExample(tEnv);

        // 2. 示例2 基于处理时间的滚动窗口
        //runProcessTimeExample(tEnv);
    }

    // 示例1 基于事件时间的滚动窗口
    private static void runEventTimeExample(TableEnvironment tEnv) {
        // 创建输入表
//        tEnv.executeSql("CREATE TABLE user_behavior_event_time (\n" +
//                "  uid BIGINT COMMENT '用户Id',\n" +
//                "  pid BIGINT COMMENT '商品Id',\n" +
//                "  cid BIGINT COMMENT '商品类目Id',\n" +
//                "  type STRING COMMENT '行为类型',\n" +
//                "  ts BIGINT COMMENT '行为时间',\n" +
//                "  ts_ltz AS TO_TIMESTAMP_LTZ(ts, 3), -- 事件时间\n" +
//                "  WATERMARK FOR ts_ltz AS ts_ltz - INTERVAL '1' MINUTE -- 在 ts_ltz 上定义watermark，ts_ltz 成为事件时间列\n" +
//                ") WITH (\n" +
//                "  'connector' = 'kafka',\n" +
//                "  'topic' = 'user_behavior',\n" +
//                "  'properties.bootstrap.servers' = 'localhost:9092',\n" +
//                "  'properties.group.id' = 'group-window-tumble-event-time',\n" +
//                "  'scan.startup.mode' = 'earliest-offset',\n" +
//                "  'format' = 'json',\n" +
//                "  'json.ignore-parse-errors' = 'false',\n" +
//                "  'json.fail-on-missing-field' = 'true'\n" +
//                ")");

        tEnv.executeSql("CREATE TABLE user_behavior_event_time (\n" +
                "  uid BIGINT COMMENT '用户Id',\n" +
                "  pid BIGINT COMMENT '商品Id',\n" +
                "  cid BIGINT COMMENT '商品类目Id',\n" +
                "  `type` STRING COMMENT '行为类型',\n" +
                "  ts BIGINT COMMENT '行为时间戳',\n" +
                "  ts_ltz AS TO_TIMESTAMP_LTZ(ts, 3), -- 事件时间\n" +
                "  WATERMARK FOR ts_ltz AS ts_ltz - INTERVAL '1' MINUTE\n" +
                ")\n" +
                "WITH (\n" +
                "  'connector' = 'filesystem',\n" +
                "  'path' = 'file:///opt/data/user_behavior.csv',\n" +
                "  'format' = 'csv'\n" +
                ")");

        tEnv.executeSql("CREATE TABLE user_behavior_event_time_uv (\n" +
                "  window_start TIMESTAMP(3) COMMENT '窗口开始时间',\n" +
                "  window_end TIMESTAMP(3) COMMENT '窗口结束时间',\n" +
                "  uv BIGINT COMMENT '用户数'\n" +
                ") WITH (\n" +
                "  'connector' = 'print',\n" +
                "  'print-identifier' = 'ET',\n" +
                "  'sink.parallelism' = '1'\n" +
                ")");
        // 执行计算并输出
        tEnv.executeSql("INSERT INTO user_behavior_event_time_uv\n" +
                "SELECT\n" +
                "  TUMBLE_START(ts_ltz, INTERVAL '1' HOUR) AS window_start,\n" +
                "  TUMBLE_END(ts_ltz, INTERVAL '1' HOUR) AS window_end,\n" +
                "  COUNT(DISTINCT uid) AS uv\n" +
                "FROM user_behavior_event_time\n" +
                "GROUP BY TUMBLE(ts_ltz, INTERVAL '1' HOUR)");
    }

    // 示例2 基于处理时间的滚动窗口
    private static void runProcessTimeExample(TableEnvironment tEnv) {
        tEnv.executeSql("CREATE TABLE user_behavior_process_time (\n" +
                "  uid BIGINT COMMENT '用户Id',\n" +
                "  pid BIGINT COMMENT '商品Id',\n" +
                "  cid BIGINT COMMENT '商品类目Id',\n" +
                "  type STRING COMMENT '行为类型',\n" +
                "  process_time AS PROCTIME() -- 处理时间\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'user_behavior',\n" +
                "  'properties.bootstrap.servers' = 'localhost:9092',\n" +
                "  'properties.group.id' = 'group-window-tumble-process-time',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'json',\n" +
                "  'json.ignore-parse-errors' = 'false',\n" +
                "  'json.fail-on-missing-field' = 'true'\n" +
                ")");
        tEnv.executeSql("CREATE TABLE user_behavior_process_time_uv (\n" +
                "  window_start TIMESTAMP(3) COMMENT '窗口开始时间',\n" +
                "  window_end TIMESTAMP(3) COMMENT '窗口结束时间',\n" +
                "  uv BIGINT COMMENT '用户数'\n" +
                ") WITH (\n" +
                "  'connector' = 'print',\n" +
                "  'print-identifier' = 'PT',\n" +
                "  'sink.parallelism' = '1'\n" +
                ")");
        tEnv.executeSql("INSERT INTO user_behavior_process_time_uv\n" +
                "SELECT\n" +
                "  TUMBLE_START(process_time, INTERVAL '1' HOUR) AS window_start,\n" +
                "  TUMBLE_END(process_time, INTERVAL '1' HOUR) AS window_end,\n" +
                "  COUNT(DISTINCT uid) AS uv\n" +
                "FROM user_behavior_process_time\n" +
                "GROUP BY TUMBLE(process_time, INTERVAL '1' HOUR)");
    }
}
