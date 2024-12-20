package com.flink.example.table.function.window;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * 功能：基于处理时间滚动窗口 迟到触发输出
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2022/10/6 下午4:16
 */
public class LateFireProcessTimeTumbleWindowExample {
    public static void main(String[] args) {
        // 执行环境
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();
        TableEnvironment tEnv = TableEnvironment.create(settings);
        Configuration config = tEnv.getConfig().getConfiguration();
        // 设置作业名称
        config.setString("pipeline.name", LateFireProcessTimeTumbleWindowExample.class.getSimpleName());
        // 窗口提前触发
        config.setBoolean("table.exec.emit.late-fire.enabled", true);
        config.setString("table.exec.emit.late-fire.delay", "10s");

        // 创建输入表
        tEnv.executeSql("CREATE TABLE user_behavior (\n" +
                "  uid BIGINT COMMENT '用户Id',\n" +
                "  pid BIGINT COMMENT '商品Id',\n" +
                "  type STRING COMMENT '行为类型',\n" +
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
                "  cnt BIGINT COMMENT '次数'\n" +
                ") WITH (\n" +
                "  'connector' = 'print'\n" +
                ")");

        // 执行计算并输出
        tEnv.executeSql("INSERT INTO user_behavior_cnt\n" +
                "SELECT\n" +
                "  DATE_FORMAT(TUMBLE_START(process_time, INTERVAL '1' MINUTE), 'yyyy-MM-dd HH:mm:ss') AS window_start,\n" +
                "  DATE_FORMAT(TUMBLE_END(process_time, INTERVAL '1' MINUTE), 'yyyy-MM-dd HH:mm:ss') AS window_end,\n" +
                "  COUNT(*) AS cnt\n" +
                "FROM user_behavior\n" +
                "GROUP BY TUMBLE(process_time, INTERVAL '1' MINUTE)");
    }
}
