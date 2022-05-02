package com.flink.example.table.dataType;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * 功能：Interval 数据类型
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/5/2 下午3:45
 */
public class IntervalExample {
    public static void main(String[] args) {
        // TableEnvironment
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        // 创建输出表
        String sinkSql = "CREATE TABLE interval_sink_table (\n" +
                "  interval_year_value TIMESTAMP(3),\n" +
                "  interval_year_p_value TIMESTAMP(3),\n" +
                "  interval_year_p_to_month_value TIMESTAMP(3),\n" +
                "  interval_month_value TIMESTAMP(3),\n" +
                "  interval_day_value TIMESTAMP(3),\n" +
                "  interval_day_p1_value TIMESTAMP(3),\n" +
                "  interval_day_p1_to_hour_value TIMESTAMP(3),\n" +
                "  interval_day_p1_to_minute_value TIMESTAMP(3),\n" +
                "  interval_day_p1_to_second_p2_value TIMESTAMP(3),\n" +
                "  interval_hour_value TIMESTAMP(3),\n" +
                "  interval_hour_to_minute_value TIMESTAMP(3),\n" +
                "  interval_hour_to_second_value TIMESTAMP(3),\n" +
                "  interval_minute_value TIMESTAMP(3),\n" +
                "  interval_minute_to_second_p2_value TIMESTAMP(3),\n" +
                "  interval_second_value TIMESTAMP(3),\n" +
                "  interval_second_p2_value TIMESTAMP(3)\n" +
                ") WITH (\n" +
                "  'connector' = 'print',\n" +
                "  'print-identifier' = 'interval',\n" +
                "  'sink.parallelism' = '1'\n" +
                ")";
        tableEnv.executeSql(sinkSql);

        // 执行计算并输出
        String sql = "INSERT INTO interval_sink_table\n" +
                "SELECT\n" +
                "    -- 1. 年-月。取值范围为 [-9999-11, +9999-11]，其中 p 是指有效位数，取值范围 [1, 4]，默认值为 2。比如如果值为 1000，但是 p = 2，则会直接报错。\n" +
                "    -- INTERVAL YEAR\n" +
                "    f1 + INTERVAL '10' YEAR AS interval_year\n" +
                "    -- INTERVAL YEAR(p)\n" +
                "    , f1 + INTERVAL '100' YEAR(3) AS interval_year_p\n" +
                "    -- INTERVAL YEAR(p) TO MONTH\n" +
                "    , f1 + INTERVAL '10-03' YEAR(3) TO MONTH AS interval_year_p_to_month\n" +
                "    -- INTERVAL MONTH\n" +
                "    , f1 + INTERVAL '13' MONTH AS interval_month\n" +
                "\n" +
                "    -- 2. 日-小时-秒。取值范围为 [-999999 23:59:59.999999999, +999999 23:59:59.999999999]，其中 p1\\p2 都是有效位数，p1 取值范围 [1, 6]，默认值为 2；p2 取值范围 [0, 9]，默认值为 6\n" +
                "    -- INTERVAL DAY\n" +
                "    , f1 + INTERVAL '10' DAY AS interval_day\n" +
                "    -- INTERVAL DAY(p1)\n" +
                "    , f1 + INTERVAL '100' DAY(3) AS interval_day_p1\n" +
                "    -- INTERVAL DAY(p1) TO HOUR\n" +
                "    , f1 + INTERVAL '10 03' DAY(3) TO HOUR AS interval_day_p1_to_hour\n" +
                "    -- INTERVAL DAY(p1) TO MINUTE\n" +
                "    , f1 + INTERVAL '10 03:12' DAY(3) TO MINUTE AS interval_day_p1_to_minute\n" +
                "    -- INTERVAL DAY(p1) TO SECOND(p2)\n" +
                "    , f1 + INTERVAL '10 00:00:00.004' DAY TO SECOND(3) AS interval_day_p1_to_second_p2\n" +
                "    -- INTERVAL HOUR\n" +
                "    , f1 + INTERVAL '10' HOUR AS interval_hour\n" +
                "    -- INTERVAL HOUR TO MINUTE\n" +
                "    , f1 + INTERVAL '10:03' HOUR TO MINUTE AS interval_hour_to_minute\n" +
                "    -- INTERVAL HOUR TO SECOND(p2)\n" +
                "    , f1 + INTERVAL '00:00:00.004' HOUR TO SECOND(3) AS interval_hour_to_second\n" +
                "    -- INTERVAL MINUTE\n" +
                "    , f1 + INTERVAL '10' MINUTE AS interval_minute\n" +
                "    -- INTERVAL MINUTE TO SECOND(p2)\n" +
                "    , f1 + INTERVAL '05:05.006' MINUTE TO SECOND(3) AS interval_minute_to_second_p2\n" +
                "    -- INTERVAL SECOND\n" +
                "    , f1 + INTERVAL '3' SECOND AS interval_second\n" +
                "    -- INTERVAL SECOND(p2)\n" +
                "    , f1 + INTERVAL '300' SECOND(3) AS interval_second_p2\n" +
                "FROM (\n" +
                "  -- 2022-05-01 12:10:15\n" +
                "  SELECT TO_TIMESTAMP_LTZ(1651378215468, 3) AS f1\n" +
                ")";
        tableEnv.executeSql(sql);
    }
}
//2032-05-01T12:10:15.468,
//2122-05-01T12:10:15.468,
//2032-08-01T12:10:15.468,
//2023-06-01T12:10:15.468,
//2022-05-11T12:10:15.468,
//2022-08-09T12:10:15.468,
//2022-05-11T15:10:15.468,
//2022-05-11T15:22:15.468,
//2022-05-11T12:10:15.472,
//2022-05-01T22:10:15.468,
//2022-05-01T22:13:15.468,
//2022-05-01T12:10:15.472,
//2022-05-01T12:20:15.468,
//2022-05-01T12:15:20.474,
//2022-05-01T12:10:18.468,
//2022-05-01T12:15:15.468,

