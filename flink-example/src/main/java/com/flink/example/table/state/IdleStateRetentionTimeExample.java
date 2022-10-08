package com.flink.example.table.state;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * 功能：空闲状态保留时间示例
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2022/10/7 下午6:48
 */
public class IdleStateRetentionTimeExample {
    public static void main(String[] args) {
        // 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(30000);
        String checkpointPath = "hdfs://localhost:9000/flink/checkpoint";
        env.getCheckpointConfig().setCheckpointStorage(new FileSystemCheckpointStorage(checkpointPath));

        // Table 运行环境配置
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);
        // 设置状态空闲时间
        TableConfig config = tEnv.getConfig();
        config.setIdleStateRetention(Duration.ofMinutes(1));

        // 设置作业名称
        Configuration configuration = config.getConfiguration();
        configuration.setString("pipeline.name", IdleStateRetentionTimeExample.class.getSimpleName());

        // 创建输入表
        tEnv.executeSql("CREATE TABLE session_behavior (\n" +
                "  session_id STRING COMMENT '会话Id',\n" +
                "  uid BIGINT COMMENT '用户Id'\n" +
                ") WITH (\n" +
                "  'connector' = 'datagen',\n" +
                "  'rows-per-second' = '100',\n" +
                "  'fields.session_id.kind' = 'sequence',\n" +
                "  'fields.session_id.start' = '10001',\n" +
                "  'fields.session_id.end' = '99999',\n" +
                "  'fields.uid.kind' = 'random',\n" +
                "  'fields.uid.min' = '1001',\n" +
                "  'fields.uid.max' = '9999'\n" +
                ")");

        // 创建输出表
        tEnv.executeSql("CREATE TABLE session_cnt (\n" +
                "  session_id STRING COMMENT '会话Id',\n" +
                "  uid BIGINT COMMENT '用户Id'\n" +
                ") WITH (\n" +
                "  'connector' = 'print'\n" +
                ")");

        // 执行计算并输出
        tEnv.executeSql("INSERT INTO session_cnt\n" +
                "SELECT\n" +
                "  session_id, COUNT(*) AS cnt\n" +
                "FROM session_behavior\n" +
                "GROUP BY session_id");
    }
}
