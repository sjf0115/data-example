package com.flink.example.table.function.windows;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * 功能：窗口 TVF 滚动窗口
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2022/10/3 上午8:55
 */
public class EventTimeTumbleWindowTVFExample {
    public static void main(String[] args) {
        // 执行环境
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();
        TableEnvironment tEnv = TableEnvironment.create(settings);
        // 设置作业名称
        Configuration configuration = tEnv.getConfig().getConfiguration();
        configuration.setString("pipeline.name", EventTimeTumbleWindowTVFExample.class.getSimpleName());

        // 创建输入表
        tEnv.executeSql("CREATE TABLE user_behavior (\n" +
                "  uid BIGINT COMMENT '用户Id',\n" +
                "  pid BIGINT COMMENT '商品Id',\n" +
                "  cid BIGINT COMMENT '商品类目Id',\n" +
                "  type STRING COMMENT '行为类型',\n" +
                "  `timestamp` BIGINT COMMENT '行为时间',\n" +
                "  `time` STRING COMMENT '行为时间',\n" +
                "  ts_ltz AS TO_TIMESTAMP_LTZ(`timestamp`, 3), -- 事件时间\n" +
                "  WATERMARK FOR ts_ltz AS ts_ltz - INTERVAL '5' SECOND -- 在 ts_ltz 上定义watermark，ts_ltz 成为事件时间列\n" +
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
                "  window_start TIMESTAMP(3) COMMENT '窗口开始时间',\n" +
                "  window_end TIMESTAMP(3) COMMENT '窗口结束时间',\n" +
                "  cnt BIGINT COMMENT '次数',\n" +
                "  min_time STRING COMMENT '最小行为时间',\n" +
                "  max_time STRING COMMENT '最大行为时间',\n" +
                "  pid_set MULTISET<BIGINT> COMMENT '商品集合'\n" +
                ") WITH (\n" +
                "  'connector' = 'print'\n" +
                ")");

        // 执行计算并输出
        tEnv.executeSql("INSERT INTO user_behavior_cnt\n" +
                "SELECT\n" +
                "  window_start, window_end,\n" +
                "  COUNT(*) AS cnt,\n" +
                "  MIN(`time`) AS min_time,\n" +
                "  MAX(`time`) AS max_time,\n" +
                "  COLLECT(DISTINCT pid) AS pid_set\n" +
                "FROM TABLE(\n" +
                "    TUMBLE(TABLE user_behavior, DESCRIPTOR(ts_ltz), INTERVAL '1' MINUTES)\n" +
                ")\n" +
                "GROUP BY window_start, window_end");
    }
}
// 输入数据
//1001,3827899,2920476,pv,1664636572000,2022-10-01 23:02:52
//1001,3745169,2891509,pv,1664636570000,2022-10-01 23:02:50
//1001,266784,2520771,pv,1664636573000,2022-10-01 23:02:53
//1001,2286574,2465336,pv,1664636574000,2022-10-01 23:02:54
//1001,1531036,2920476,pv,1664636577000,2022-10-01 23:02:57
//1001,2266567,4145813,pv,1664636584000,2022-10-01 23:03:04
//1001,2951368,1080785,pv,1664636576000,2022-10-01 23:02:56
//1001,3658601,2342116,pv,1664636586000,2022-10-01 23:03:06
//1001,5153036,2342116,pv,1664636578000,2022-10-01 23:02:58
//1001,598929,2429887,pv,1664636591000,2022-10-01 23:03:11
//1001,3245421,2881542,pv,1664636595000,2022-10-01 23:03:15
//1001,1046201,3002561,pv,1664636579000,2022-10-01 23:02:59
//1001,2971043,4869428,pv,1664636646000,2022-10-01 23:04:06

// 输出结果
//+I[2022-10-01T23:02, 2022-10-01T23:03, 6, 2022-10-01 23:02:50, 2022-10-01 23:02:57, {3827899=1, 266784=1, 2951368=1, 3745169=1, 1531036=1, 2286574=1}]
//+I[2022-10-01T23:03, 2022-10-01T23:04, 4, 2022-10-01 23:03:04, 2022-10-01 23:03:15, {2266567=1, 598929=1, 3245421=1, 3658601=1}]