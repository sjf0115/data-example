package com.flink.example.table.function.windows;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * 功能：处理时间 分组滚动窗口 SQL 示例
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/5/16 下午10:02
 */
public class ProcessTimeGroupWindowSQLExample {
    public static void main(String[] args) {
        // 执行环境
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        // 创建输入表
        tEnv.executeSql("CREATE TABLE user_behavior (\n" +
                "  uid BIGINT COMMENT '用户Id',\n" +
                "  pid BIGINT COMMENT '商品Id',\n" +
                "  cid BIGINT COMMENT '商品类目Id',\n" +
                "  type STRING COMMENT '行为类型',\n" +
                "  ts BIGINT COMMENT '行为时间',\n" +
                "  `time` STRING COMMENT '行为时间',\n" +
                "  process_time AS PROCTIME() -- 处理时间\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'user_behavior',\n" +
                "  'properties.bootstrap.servers' = 'localhost:9092',\n" +
                "  'properties.group.id' = 'user_behavior',\n" +
                "  'scan.startup.mode' = 'latest-offset',\n" +
                "  'format' = 'json',\n" +
                "  'json.ignore-parse-errors' = 'false',\n" +
                "  'json.fail-on-missing-field' = 'true'\n" +
                ")");

        // 创建输出表
        tEnv.executeSql("CREATE TABLE user_behavior_cnt (\n" +
                "  window_start STRING COMMENT '窗口开始时间',\n" +
                "  window_end STRING COMMENT '窗口结束时间',\n" +
                "  window_start_timestamp TIMESTAMP(3) COMMENT '窗口开始时间',\n" +
                "  window_end_timestamp TIMESTAMP(3) COMMENT '窗口结束时间',\n" +
                "  cnt BIGINT COMMENT '次数',\n" +
                "  min_time STRING COMMENT '最小行为时间',\n" +
                "  max_time STRING COMMENT '最大行为时间'\n" +
                ") WITH (\n" +
                "  'connector' = 'print',\n" +
                "  'print-identifier' = 'PT'\n" +
                ")");

        // 执行计算并输出
        tEnv.executeSql("INSERT INTO user_behavior_cnt\n" +
                "SELECT\n" +
                "  DATE_FORMAT(TUMBLE_START(process_time, INTERVAL '1' HOUR), 'yyyy-MM-dd HH:mm:ss') AS window_start,\n" +
                "  DATE_FORMAT(TUMBLE_END(process_time, INTERVAL '1' HOUR), 'yyyy-MM-dd HH:mm:ss') AS window_end,\n" +
                "  TUMBLE_START(process_time, INTERVAL '1' HOUR) AS window_start_timestamp,\n" +
                "  TUMBLE_END(process_time, INTERVAL '1' HOUR) AS window_end_timestamp,\n" +
                "  COUNT(*) AS cnt,\n" +
                "  MIN(`time`) AS min_time,\n" +
                "  MAX(`time`) AS max_time\n" +
                "FROM user_behavior\n" +
                "GROUP BY TUMBLE(process_time, INTERVAL '1' HOUR)");
    }
}
// 输入
//1001,3827899,2920476,pv,1511713473000,2017-11-27 00:24:33
//1001,3745169,2891509,pv,1511714671000,2017-11-27 00:44:31
//1002,266784,2520771,pv,1511715653000,2017-11-27 01:00:53
//1002,2286574,2465336,pv,1511716407000,2017-11-27 01:13:27
//1001,1531036,2920476,pv,1511718252000,2017-11-27 01:44:12
//1001,2266567,4145813,pv,1511741471000,2017-11-27 08:11:11
//1001,2951368,1080785,pv,1511750828000,2017-11-27 10:47:08