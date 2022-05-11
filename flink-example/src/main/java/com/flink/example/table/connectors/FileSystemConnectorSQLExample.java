package com.flink.example.table.connectors;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * 功能：File Connector SQL 示例
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/5/11 下午8:57
 */
public class FileSystemConnectorSQLExample {
    public static void main(String[] args) throws Exception {
        // TableEnvironment
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .useBlinkPlanner()
                .build();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        String sourceSql = "CREATE TABLE user_behavior (\n" +
                "  uid BIGINT COMMENT '用户Id',\n" +
                "  pid BIGINT COMMENT '商品Id',\n" +
                "  cid BIGINT COMMENT '商品类目Id',\n" +
                "  `type` STRING COMMENT '行为类型',\n" +
                "  ts BIGINT COMMENT '行为时间戳',\n" +
                "  tm STRING COMMENT '行为时间',\n" +
                "  process_time AS PROCTIME() -- 处理时间\n" +
                ")\n" +
                "WITH (\n" +
                "  'connector' = 'filesystem',\n" +
                "  'path' = 'file:///opt/data/user_behavior.csv',\n" +
                "  'format' = 'csv'\n" +
                ")";
        tEnv.executeSql(sourceSql);

        // 创建 Print Sink 表
        String sinkSql = "CREATE TABLE print_sink_table (\n" +
                "  uid BIGINT COMMENT '用户Id',\n" +
                "  cnt BIGINT COMMENT '次数'\n" +
                ") WITH (\n" +
                "  'connector' = 'print',\n" +
                "  'print-identifier' = 'behavior'\n" +
                ")";
        tEnv.executeSql(sinkSql);

        // 执行计算并输出
        String sql = "INSERT INTO print_sink_table\n" +
                "SELECT uid, COUNT(*) as cnt\n" +
                "FROM user_behavior\n" +
                "GROUP BY uid";
        tEnv.executeSql(sql);
    }
}
