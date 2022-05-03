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
                "  year_1   TIMESTAMP(3),\n" +
                "  year_2   TIMESTAMP(3),\n" +
                "  year_3   TIMESTAMP(3),\n" +
                "  year_4   TIMESTAMP(3),\n" +
                "  month_1  TIMESTAMP(3),\n" +
                "  month_2  TIMESTAMP(3),\n" +
                "  month_3  TIMESTAMP(3),\n" +
                "  month_4  TIMESTAMP(3),\n" +
                "  day_1    TIMESTAMP(3),\n" +
                "  day_2    TIMESTAMP(3),\n" +
                "  day_3    TIMESTAMP(3),\n" +
                "  day_4    TIMESTAMP(3),\n" +
                "  hour_1   TIMESTAMP(3),\n" +
                "  hour_2   TIMESTAMP(3),\n" +
                "  hour_3   TIMESTAMP(3),\n" +
                "  hour_4   TIMESTAMP(3),\n" +
                "  minute_1 TIMESTAMP(3),\n" +
                "  minute_2 TIMESTAMP(3),\n" +
                "  minute_3 TIMESTAMP(3),\n" +
                "  minute_4 TIMESTAMP(3),\n" +
                "  minute_5 TIMESTAMP(3),\n" +
                "  minute_6 TIMESTAMP(3),\n" +
                "  second_1 TIMESTAMP(3),\n" +
                "  second_2 TIMESTAMP(3),\n" +
                "  second_3 TIMESTAMP(3),\n" +
                "  second_4 TIMESTAMP(3),\n" +
                "  second_5 TIMESTAMP(3),\n" +
                "  second_6 TIMESTAMP(3),\n" +
                "  second_7 TIMESTAMP(3),\n" +
                "  second_8 TIMESTAMP(3),\n" +
                "  second_9 TIMESTAMP(3)\n" +
                ") WITH (\n" +
                "  'connector' = 'print',\n" +
                "  'print-identifier' = 'interval',\n" +
                "  'sink.parallelism' = '1'\n" +
                ")";
        tableEnv.executeSql(sinkSql);

        // 执行计算并输出
        String sql = "INSERT INTO interval_sink_table\n" +
                "SELECT\n" +
                "    ts + INTERVAL '-1' YEAR                         AS year_1,    -- 1年\n" +
                "    ts + INTERVAL '+1' YEAR                         AS year_2,    -- 1年\n" +
                "    ts + INTERVAL '10' YEAR(2)                      AS year_3,    -- 10年\n" +
                "    ts + INTERVAL '1000' YEAR(4)                    AS year_4,    -- 1000年\n" +
                "    ts + INTERVAL '1-03' YEAR TO MONTH              AS month_1,   -- 1年3个月\n" +
                "    ts + INTERVAL '15' MONTH                        AS month_2,   -- 15个月\n" +
                "    ts + INTERVAL '+3' MONTH                        AS month_3,   -- 3个月\n" +
                "    ts + INTERVAL '-3' MONTH                        AS month_4,   -- 3个月\n" +
                "    ts + INTERVAL '+1' DAY                          AS day_1,     -- 1天\n" +
                "    ts + INTERVAL '-1' DAY                          AS day_2,     -- 1天\n" +
                "    ts + INTERVAL '10' DAY(2)                       AS day_3,     -- 10天\n" +
                "    ts + INTERVAL '365' DAY(3)                      AS day_4,     -- 365天\n" +
                "    ts + INTERVAL '-1 03' DAY TO HOUR               AS hour_1,    -- 1天3小时\n" +
                "    ts + INTERVAL '1 03' DAY TO HOUR                AS hour_2,    -- 1天3小时\n" +
                "    ts + INTERVAL '10 03' DAY(2) TO HOUR            AS hour_3,    -- 10天3小时\n" +
                "    ts + INTERVAL '+3' HOUR                         AS hour_4,    -- 3小时\n" +
                "    ts + INTERVAL '-3' HOUR                         AS hour_5,    -- 3小时\n" +
                "    ts + INTERVAL '1 03:20' DAY TO MINUTE           AS minute_1,  -- 1天3小时20分钟\n" +
                "    ts + INTERVAL '-1 03:20' DAY TO MINUTE          AS minute_2,  -- 1天3小时20分钟\n" +
                "    ts + INTERVAL '3:20' HOUR TO MINUTE             AS minute_3,  -- 3小时20分钟\n" +
                "    ts + INTERVAL '-3:20' HOUR TO MINUTE            AS minute_4,  -- 3小时20分钟\n" +
                "    ts + INTERVAL '+20' MINUTE                      AS minute_5,  -- 20分钟\n" +
                "    ts + INTERVAL '-20' MINUTE                      AS minute_6,  -- 20分钟\n" +
                "    ts + INTERVAL '1 03:20:15' DAY TO SECOND        AS second_1,  -- 1天3小时20分钟15秒\n" +
                "    ts + INTERVAL '1 03:20:15.111' DAY TO SECOND(3) AS second_2,  -- 1天3小时20分钟15秒111毫秒\n" +
                "    ts + INTERVAL '3:20:15' HOUR TO SECOND          AS second_3,  -- 3小时20分钟15秒\n" +
                "    ts + INTERVAL '3:20:15.111' HOUR TO SECOND(3)   AS second_4,  -- 3小时20分钟15秒111毫秒\n" +
                "    ts + INTERVAL '20:15' MINUTE TO SECOND          AS second_5,  -- 20分钟15秒\n" +
                "    ts + INTERVAL '20:15.111' MINUTE TO SECOND(3)   AS second_6,  -- 20分钟15秒111毫秒\n" +
                "    ts + INTERVAL '15' SECOND                       AS second_7,  -- 15秒\n" +
                "    ts + INTERVAL '-15' SECOND                      AS second_8,  -- 15秒\n" +
                "    ts + INTERVAL '15.111' SECOND(3)                AS second_9   -- 15秒111毫秒\n" +
                "FROM (\n" +
                "  -- 2022-05-01 12:10:15.456\n" +
                "  SELECT TO_TIMESTAMP_LTZ(1651378215456, 3) AS ts\n" +
                ")";
        tableEnv.executeSql(sql);
    }
}

