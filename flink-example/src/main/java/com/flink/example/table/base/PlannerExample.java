package com.flink.example.table.base;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 功能：Planner 示例
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/4/12 下午11:32
 */
public class PlannerExample {
    public static void main(String[] args) {
        // TableEnvironment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);
        // TableEnvironment tEnv = TableEnvironment.create(settings);


        // TableEnvironment
//        EnvironmentSettings settings = EnvironmentSettings
//                .newInstance()
//                .useOldPlanner()
//                .inStreamingMode()
//                .build();
//        TableEnvironment tEnv = TableEnvironment.create(settings);
    }
}
