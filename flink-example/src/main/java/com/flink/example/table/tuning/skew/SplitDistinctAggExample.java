package com.flink.example.table.tuning.skew;

import com.flink.example.stream.source.simple.SkewUserMockSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * 功能：拆分 Distinct 聚合优化
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2022/10/16 下午6:09
 */
public class SplitDistinctAggExample {
    public static void main(String[] args) {
        // 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        // 状态后端
        env.setStateBackend(new HashMapStateBackend());

        // Checkpoint
        env.getCheckpointConfig().setCheckpointInterval(10000);
        String checkpointPath = "hdfs://localhost:9000/flink/checkpoint";
        env.getCheckpointConfig().setCheckpointStorage(new FileSystemCheckpointStorage(checkpointPath));

        // Table 运行环境配置
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        Configuration configuration = tEnv.getConfig().getConfiguration();
        // 设置作业名称
        configuration.setString("pipeline.name", SplitDistinctAggExample.class.getSimpleName());
        // 作业链
        configuration.setString("pipeline.operator-chaining", "false");

        // 开启 MiniBatch
        configuration.setString("table.exec.mini-batch.enabled", "true");
        configuration.setString("table.exec.mini-batch.allow-latency", "1 s");
        configuration.setString("table.exec.mini-batch.size", "5000");

        // 开启拆分 Distinct 优化
        configuration.setString("table.optimizer.distinct-agg.split.enabled", "true");
        configuration.setString("table.optimizer.distinct-agg.split.bucket-num", "1024");

        // 行为流 (behavior, uid)
        DataStreamSource<Tuple2<String, String>> source = env.addSource(new SkewUserMockSource()).setParallelism(2);
        // Stream 转 Table
        tEnv.createTemporaryView("user_behavior", source, $("behavior"), $("uid"));

        // 创建输出表
        tEnv.executeSql("CREATE TABLE behavior_uv (\n" +
                "  behavior STRING COMMENT '行为类型',\n" +
                "  uv BIGINT COMMENT '用户数'\n" +
                ") WITH (\n" +
                "  'connector' = 'print'\n" +
                ")");

        // 执行计算并输出
        tEnv.executeSql("INSERT INTO behavior_uv\n" +
                "SELECT\n" +
                "  behavior, COUNT(DISTINCT uid) AS uv\n" +
                "FROM user_behavior\n" +
                "GROUP BY behavior");
    }
}
