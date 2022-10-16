package com.flink.example.table.tuning.skew;

import com.flink.example.stream.source.simple.SkewWordMockSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 功能：
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2022/10/16 下午12:40
 */
public class LocalGlobalAggExample {
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
        configuration.setString("pipeline.name", LocalGlobalAggExample.class.getSimpleName());
        // 开启 Checkpoint
        configuration.setString("execution.checkpointing.interval", "20s");

        // 开启 MiniBatch
        configuration.setString("table.exec.mini-batch.enabled", "true");
        configuration.setString("table.exec.mini-batch.allow-latency", "1 s");
        configuration.setString("table.exec.mini-batch.size", "5");
        // 开启两阶段聚合
        configuration.setString("table.optimizer.agg-phase-strategy", "TWO_PHASE");

        // 单词流 (word, 1)
        DataStreamSource<Tuple2<String, Integer>> source = env.addSource(new SkewWordMockSource());
        // Stream 转 Table
        tEnv.createTemporaryView("words", source);

        // 创建输出表
        tEnv.executeSql("CREATE TABLE word_count (\n" +
                "  word BIGINT COMMENT '单词',\n" +
                "  count BIGINT COMMENT '出现次数'\n" +
                ") WITH (\n" +
                "  'connector' = 'print'\n" +
                ")");

        // 执行计算并输出
        tEnv.executeSql("INSERT INTO word_count\n" +
                "SELECT\n" +
                "  word, COUNT(*) AS cnt\n" +
                "FROM words\n" +
                "GROUP BY word");
    }
}
