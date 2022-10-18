package com.flink.example.table.function.top;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 功能：TopN 示例
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2022/10/18 上午8:20
 */
public class TopNExample {
    public static void main(String[] args) {
        // 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        // 状态后端
        env.setStateBackend(new HashMapStateBackend());
        // 开启 Checkpoint
        env.enableCheckpointing(10000);
        String checkpointPath = "hdfs://localhost:9000/flink/checkpoint";
        env.getCheckpointConfig().setCheckpointStorage(new FileSystemCheckpointStorage(checkpointPath));

        // Table 执行环境
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);
        Configuration config = tEnv.getConfig().getConfiguration();
        // 设置作业名称
        config.setString("pipeline.name", TopNExample.class.getSimpleName());

        // 创建输入表
        tEnv.executeSql("CREATE TABLE shop_sales (\n" +
                "  product_id BIGINT COMMENT '商品Id',\n" +
                "  category_id BIGINT COMMENT '商品类目Id',\n" +
                "  sales BIGINT COMMENT '下单量',\n" +
                "  process_time AS PROCTIME() -- 处理时间\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'shop_sales',\n" +
                "  'properties.bootstrap.servers' = 'localhost:9092',\n" +
                "  'properties.group.id' = 'user_behavior',\n" +
                "  'scan.startup.mode' = 'latest-offset',\n" +
                "  'format' = 'json',\n" +
                "  'json.ignore-parse-errors' = 'false',\n" +
                "  'json.fail-on-missing-field' = 'true'\n" +
                ")");

        // 创建输出表
        tEnv.executeSql("CREATE TABLE shop_sales_top (\n" +
                "  category_id BIGINT COMMENT '商品类目Id',\n" +
                "  product_id BIGINT COMMENT '商品Id',\n" +
                "  sales BIGINT COMMENT '下单量',\n" +
                "  row_num BIGINT COMMENT '排名'\n" +
                ") WITH (\n" +
                "  'connector' = 'print'\n" +
                ")");

        // 执行计算并输出
        tEnv.executeSql("INSERT INTO shop_sales_top\n" +
                "SELECT\n" +
                "  category_id, product_id, sales, row_num\n" +
                "FROM (\n" +
                "  SELECT\n" +
                "    category_id, product_id, sales,\n" +
                "    ROW_NUMBER() OVER (PARTITION BY category_id ORDER BY sales DESC) AS row_num\n" +
                "  FROM shop_sales\n" +
                ")\n" +
                "WHERE row_num <= 5");
    }
}
