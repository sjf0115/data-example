package com.flink.example.table;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * 功能：TableEnvironmentExample
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/4/5 下午6:55
 */
public class TableEnvironmentExample {
    public static void main(String[] args) {

        // Streaming
        EnvironmentSettings settings1 = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();

        TableEnvironment tableEnv1 = TableEnvironment.create(settings1);

        // Batch
        EnvironmentSettings settings2 = EnvironmentSettings
                .newInstance()
                .inBatchMode()
                .build();

        TableEnvironment tableEnv2 = TableEnvironment.create(settings2);
    }
}
