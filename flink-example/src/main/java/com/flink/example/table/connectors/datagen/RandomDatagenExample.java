package com.flink.example.table.connectors.datagen;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * 功能：Datagen Connector 随机生成器 示例
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2022/10/7 下午9:48
 */
public class RandomDatagenExample {
    public static void main(String[] args) {
        // 执行环境
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();
        TableEnvironment tEnv = TableEnvironment.create(settings);
        // 设置作业名称
        Configuration configuration = tEnv.getConfig().getConfiguration();
        configuration.setString("pipeline.name", RandomDatagenExample.class.getSimpleName());

        // 创建输入表
        tEnv.executeSql("CREATE TABLE order_behavior (\n" +
                "  order_id STRING COMMENT '订单Id',\n" +
                "  uid BIGINT COMMENT '用户Id',\n" +
                "  amount DOUBLE COMMENT '订单金额',\n" +
                "  `timestamp` TIMESTAMP_LTZ(3) COMMENT '下单时间戳',\n" +
                "  WATERMARK FOR `timestamp` AS `timestamp` - INTERVAL '5' SECOND -- 在 ts_ltz 上定义watermark，ts_ltz 成为事件时间列\n" +
                ") WITH (\n" +
                "  'connector' = 'datagen',\n" +
                "  'rows-per-second' = '1',\n" +
                "  'fields.order_id.kind' = 'random',\n" +
                "  'fields.order_id.length' = '6',\n" +
                "  'fields.uid.kind' = 'random',\n" +
                "  'fields.uid.min' = '10000001',\n" +
                "  'fields.uid.max' = '99999999',\n" +
                "  'fields.amount.kind' = 'random',\n" +
                "  'fields.amount.min' = '1',\n" +
                "  'fields.amount.max' = '1000'\n" +
                ")");

        // 创建输出表
        tEnv.executeSql("CREATE TABLE order_behavior_print (\n" +
                "  order_id STRING COMMENT '订单Id',\n" +
                "  uid BIGINT COMMENT '用户Id',\n" +
                "  amount DOUBLE COMMENT '订单金额',\n" +
                "  `timestamp` TIMESTAMP_LTZ(3) COMMENT '下单时间戳',\n" +
                "  `time` STRING COMMENT '下单时间'\n" +
                ") WITH (\n" +
                "  'connector' = 'print'\n" +
                ")");

        // 执行计算并输出
        tEnv.executeSql("INSERT INTO order_behavior_print\n" +
                "SELECT\n" +
                "  order_id, uid, amount, `timestamp`,\n" +
                "  DATE_FORMAT(`timestamp`, 'yyyy-MM-dd HH:mm:ss') AS `time`\n" +
                "FROM order_behavior");
    }
}
