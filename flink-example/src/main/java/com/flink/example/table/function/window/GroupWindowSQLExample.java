package com.flink.example.table.function.window;

import com.common.example.bean.UserBehavior;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

/**
 * 功能：Group Windows SQL 示例
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/5/3 下午9:29
 */
public class GroupWindowSQLExample {

    private static final Logger LOG = LoggerFactory.getLogger(GroupWindowSQLExample.class);

    public static void main(String[] args) throws Exception {
        // Table 运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        // Socket 数据源
        DataStream<String> source = env.socketTextStream("localhost", 9100, "\n");
        DataStream<UserBehavior> mapStream = source.map(new MapFunction<String, UserBehavior>() {
            @Override
            public UserBehavior map(String value) throws Exception {
                String[] params = value.split(",");
                Long uid = Long.parseLong(params[0]);
                Long pid = Long.parseLong(params[1]);
                Long cid = Long.parseLong(params[2]);
                String type = params[3];
                Long ts = Long.parseLong(params[4]);
                String tm = params[5];
                LOG.info("[INFO] behavior uid: {}, ts: {}, tm: {}", uid, ts, tm);
                return new UserBehavior(uid, pid, cid, type, ts, tm);
            }
        });

        // 1. 示例1 基于事件时间的滚动窗口
        runEventTimeExample(tEnv, mapStream);

        // 2. 示例2 基于处理时间的滚动窗口
        //runProcessTimeExample(tEnv);

        env.execute("GroupWindowSQLExample");
    }

    // 示例1 基于事件时间的滚动窗口
    private static void runEventTimeExample(StreamTableEnvironment tEnv, DataStream<UserBehavior> mapStream) {

        // 提取时间戳并分配 Watermark
        DataStream<UserBehavior> behaviorStream = mapStream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<UserBehavior>forBoundedOutOfOrderness(Duration.ofMinutes(1))
                        .withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
                            @Override
                            public long extractTimestamp(UserBehavior behavior, long recordTimestamp) {
                                return behavior.getTimestamp();
                            }
                        })
        );

        // 注册虚拟表
        Table behaviorTable = tEnv.fromDataStream(behaviorStream, $("uid"), $("ts").rowtime());
        tEnv.createTemporaryView("user_behavior", behaviorTable);

        // 创建输入表
//        tEnv.executeSql("CREATE TABLE user_behavior_event_time (\n" +
//                "  uid BIGINT COMMENT '用户Id',\n" +
//                "  pid BIGINT COMMENT '商品Id',\n" +
//                "  cid BIGINT COMMENT '商品类目Id',\n" +
//                "  type STRING COMMENT '行为类型',\n" +
//                "  ts BIGINT COMMENT '行为时间',\n" +
//                "  ts_ltz AS TO_TIMESTAMP_LTZ(ts, 3), -- 事件时间\n" +
//                "  WATERMARK FOR ts_ltz AS ts_ltz - INTERVAL '1' MINUTE -- 在 ts_ltz 上定义watermark，ts_ltz 成为事件时间列\n" +
//                ") WITH (\n" +
//                "  'connector' = 'kafka',\n" +
//                "  'topic' = 'user_behavior',\n" +
//                "  'properties.bootstrap.servers' = 'localhost:9092',\n" +
//                "  'properties.group.id' = 'group-window-tumble-event-time',\n" +
//                "  'scan.startup.mode' = 'earliest-offset',\n" +
//                "  'format' = 'json',\n" +
//                "  'json.ignore-parse-errors' = 'false',\n" +
//                "  'json.fail-on-missing-field' = 'true'\n" +
//                ")");

//        tEnv.executeSql("CREATE TABLE user_behavior_event_time (\n" +
//                "  uid BIGINT COMMENT '用户Id',\n" +
//                "  pid BIGINT COMMENT '商品Id',\n" +
//                "  cid BIGINT COMMENT '商品类目Id',\n" +
//                "  `type` STRING COMMENT '行为类型',\n" +
//                "  ts BIGINT COMMENT '行为时间戳',\n" +
//                "  tm STRING COMMENT '行为时间',\n" +
//                "  ts_ltz AS TO_TIMESTAMP_LTZ(ts, 3), -- 事件时间\n" +
//                "  WATERMARK FOR ts_ltz AS ts_ltz - INTERVAL '1' MINUTE\n" +
//                ")\n" +
//                "WITH (\n" +
//                "  'connector' = 'filesystem',\n" +
//                "  'path' = 'file:///opt/data/user_behavior.csv',\n" +
//                "  'format' = 'csv'\n" +
//                ")");

//        tEnv.executeSql("CREATE TABLE user_behavior_uv (\n" +
//                "  window_start STRING COMMENT '窗口开始时间',\n" +
//                "  window_end STRING COMMENT '窗口结束时间',\n" +
//                "  cnt BIGINT COMMENT '次数'\n" +
//                ") WITH (\n" +
//                "  'connector' = 'print',\n" +
//                "  'print-identifier' = 'ET',\n" +
//                "  'sink.parallelism' = '1'\n" +
//                ")");

        tEnv.executeSql("CREATE TABLE user_behavior_uv (\n" +
                "  window_start TIMESTAMP(3) COMMENT '窗口开始时间',\n" +
                "  window_end TIMESTAMP(3) COMMENT '窗口结束时间',\n" +
                "  cnt BIGINT COMMENT '次数'\n" +
                ") WITH (\n" +
                "  'connector' = 'print',\n" +
                "  'print-identifier' = 'ET',\n" +
                "  'sink.parallelism' = '1'\n" +
                ")");

        // 执行计算并输出
//        tEnv.executeSql("INSERT INTO user_behavior_uv\n" +
//                "SELECT\n" +
//                "  DATE_FORMAT(TUMBLE_START(ts, INTERVAL '1' HOUR), 'yyyy-MM-dd HH:mm:ss') AS window_start,\n" +
//                "  DATE_FORMAT(TUMBLE_END(ts, INTERVAL '1' HOUR), 'yyyy-MM-dd HH:mm:ss') AS window_end,\n" +
//                "  COUNT(*) AS cnt\n" +
//                "FROM user_behavior\n" +
//                "GROUP BY TUMBLE(ts, INTERVAL '1' HOUR)");


        tEnv.executeSql("INSERT INTO user_behavior_uv\n" +
                "SELECT\n" +
                "  TUMBLE_START(ts, INTERVAL '1' HOUR) AS window_start,\n" +
                "  TUMBLE_END(ts, INTERVAL '1' HOUR) AS window_end,\n" +
                "  COUNT(*) AS cnt\n" +
                "FROM user_behavior\n" +
                "GROUP BY TUMBLE(ts, INTERVAL '1' HOUR)");
    }

    // 示例2 基于处理时间的滚动窗口
    private static void runProcessTimeExample(TableEnvironment tEnv) {
        tEnv.executeSql("CREATE TABLE user_behavior_process_time (\n" +
                "  uid BIGINT COMMENT '用户Id',\n" +
                "  pid BIGINT COMMENT '商品Id',\n" +
                "  cid BIGINT COMMENT '商品类目Id',\n" +
                "  type STRING COMMENT '行为类型',\n" +
                "  process_time AS PROCTIME() -- 处理时间\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'user_behavior',\n" +
                "  'properties.bootstrap.servers' = 'localhost:9092',\n" +
                "  'properties.group.id' = 'group-window-tumble-process-time',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'json',\n" +
                "  'json.ignore-parse-errors' = 'false',\n" +
                "  'json.fail-on-missing-field' = 'true'\n" +
                ")");
        tEnv.executeSql("CREATE TABLE user_behavior_process_time_uv (\n" +
                "  window_start TIMESTAMP(3) COMMENT '窗口开始时间',\n" +
                "  window_end TIMESTAMP(3) COMMENT '窗口结束时间',\n" +
                "  uv BIGINT COMMENT '用户数'\n" +
                ") WITH (\n" +
                "  'connector' = 'print',\n" +
                "  'print-identifier' = 'PT',\n" +
                "  'sink.parallelism' = '1'\n" +
                ")");
        tEnv.executeSql("INSERT INTO user_behavior_process_time_uv\n" +
                "SELECT\n" +
                "  TUMBLE_START(process_time, INTERVAL '1' HOUR) AS window_start,\n" +
                "  TUMBLE_END(process_time, INTERVAL '1' HOUR) AS window_end,\n" +
                "  COUNT(DISTINCT uid) AS uv\n" +
                "FROM user_behavior_process_time\n" +
                "GROUP BY TUMBLE(process_time, INTERVAL '1' HOUR)");
    }
}
