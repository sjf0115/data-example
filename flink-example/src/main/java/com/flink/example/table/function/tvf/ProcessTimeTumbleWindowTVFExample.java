package com.flink.example.table.function.tvf;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * 功能：窗口 TVF 基于处理时间的滚动窗口 1分钟内一个窗口
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2022/10/3 上午8:55
 */
public class ProcessTimeTumbleWindowTVFExample {
    public static void main(String[] args) {
        // 执行环境
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();
        TableEnvironment tEnv = TableEnvironment.create(settings);
        // 设置作业名称
        Configuration configuration = tEnv.getConfig().getConfiguration();
        configuration.setString("pipeline.name", ProcessTimeTumbleWindowTVFExample.class.getSimpleName());

        // 创建输入表
        tEnv.executeSql("CREATE TABLE user_behavior (\n" +
                "  uid BIGINT COMMENT '用户Id',\n" +
                "  pid BIGINT COMMENT '商品Id',\n" +
                "  cid BIGINT COMMENT '商品类目Id',\n" +
                "  type STRING COMMENT '行为类型',\n" +
                "  `timestamp` BIGINT COMMENT '行为时间',\n" +
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
                "    TUMBLE(TABLE user_behavior, DESCRIPTOR(process_time), INTERVAL '1' MINUTES)\n" +
                ")\n" +
                "GROUP BY window_start, window_end");
    }
}
