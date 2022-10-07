package com.flink.example.table.connectors.datagen;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * 功能：Datagen Connector 序列生成器
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2022/10/7 下午10:29
 */
public class SequenceDatagenExample {
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
                "  order_id BIGINT COMMENT '订单Id'\n" +
                ") WITH (\n" +
                "  'connector' = 'datagen',\n" +
                "  'rows-per-second' = '1',\n" +
                "  -- order_id\n" +
                "  'fields.order_id.kind' = 'sequence',\n" +
                "  'fields.order_id.start' = '10000001',\n" +
                "  'fields.order_id.end' = '10000010'\n" +
                ")");

        // 创建输出表
        tEnv.executeSql("CREATE TABLE order_behavior_print (\n" +
                "  order_id BIGINT COMMENT '订单Id'\n" +
                ") WITH (\n" +
                "  'connector' = 'print'\n" +
                ")");

        // 执行计算并输出
        tEnv.executeSql("INSERT INTO order_behavior_print\n" +
                "SELECT\n" +
                "  order_id\n" +
                "FROM order_behavior");
    }
}
