package com.flink.example.table.connectors;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * 功能：FileSystem Connector SQL 示例
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/4/19 下午11:24
 */
public class FileSystemConnectorSQLExample {
    public static void main(String[] args) throws Exception {
        // TableEnvironment
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .useOldPlanner()
                .inStreamingMode()
                .build();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        String sourceSQL= "CREATE TABLE file_source_table (\n" +
                "  word STRING COMMENT '单词',\n" +
                "  frequency bigint COMMENT '次数'\n" +
                ") WITH (\n" +
                "  'connector.type' = 'filesystem',\n" +
                "  'connector.path' = '/opt/data/word_count_input.csv',\n" +
                "  'format.type' = 'csv',\n" +
                "  'format.fields.0.name' = 'word',\n" +
                "  'format.fields.0.data-type' = 'STRING',\n" +
                "  'format.fields.1.name' = 'frequency',\n" +
                "  'format.fields.1.data-type' = 'bigint'\n" +
                ")";
        tEnv.sqlUpdate(sourceSQL);

        String sinkSQL = "CREATE TABLE file_sink_table (\n" +
                "  word STRING COMMENT '单词',\n" +
                "  frequency bigint COMMENT '次数'\n" +
                ") WITH (\n" +
                "  'connector.type' = 'filesystem',\n" +
                "  'connector.path' = '/opt/data/word_count_output.csv',\n" +
                "  'format.type' = 'csv',\n" +
                "  'format.fields.0.name' = 'word',\n" +
                "  'format.fields.0.data-type' = 'STRING',\n" +
                "  'format.fields.1.name' = 'frequency',\n" +
                "  'format.fields.1.data-type' = 'bigint'\n" +
                ")";
        tEnv.sqlUpdate(sinkSQL);

        String querySQL = "INSERT INTO file_sink_table\n" +
                "SELECT word, SUM(frequency) AS frequency\n" +
                "FROM file_source_table\n" +
                "GROUP BY word";
        tEnv.sqlUpdate(querySQL);

        tEnv.execute("FileSystemConnectorSQLExample");
    }
}
