package com.flink.example.table.state;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 功能：Group 未配置空闲状态时间
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/5/16 下午10:02
 */
public class GroupWithoutIdleExample {
    public static void main(String[] args) {
        // 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStateBackend(new HashMapStateBackend());
        String checkpointPath = "hdfs://localhost:9000/flink/checkpoint";
        env.getCheckpointConfig().setCheckpointStorage(new FileSystemCheckpointStorage(checkpointPath));

        // Table 运行环境配置
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);
        // 设置作业名称
        Configuration configuration = tEnv.getConfig().getConfiguration();
        configuration.setString("pipeline.name", GroupWithoutIdleExample.class.getSimpleName());
        // 开启 Checkpoint
        configuration.setString("execution.checkpointing.interval", "20s");

        // 创建输入表
        tEnv.executeSql("CREATE TABLE order_behavior (\n" +
                "  order_id STRING COMMENT '订单Id',\n" +
                "  uid BIGINT COMMENT '用户Id',\n" +
                "  amount DOUBLE COMMENT '订单金额',\n" +
                "  `timestamp` TIMESTAMP_LTZ(3) COMMENT '下单时间戳',\n" +
                "  WATERMARK FOR `timestamp` AS `timestamp` - INTERVAL '5' SECOND -- 在 ts_ltz 上定义watermark，ts_ltz 成为事件时间列\n" +
                ") WITH (\n" +
                "  'connector' = 'datagen',\n" +
                "  'rows-per-second' = '5000',\n" +
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
        tEnv.executeSql("CREATE TABLE order_cnt (\n" +
                "  uid BIGINT COMMENT '用户Id',\n" +
                "  cnt BIGINT COMMENT '下单次数'\n" +
                ") WITH (\n" +
                "  'connector' = 'print'\n" +
                ")");

        // 执行计算并输出
        tEnv.executeSql("INSERT INTO order_cnt\n" +
                "SELECT\n" +
                "  uid, COUNT(*) AS cnt\n" +
                "FROM order_behavior\n" +
                "GROUP BY uid");
    }
}