package com.flink.example.table.time;

import com.common.example.bean.UserBehavior;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.Duration;
import java.time.ZoneId;

import static org.apache.flink.table.api.Expressions.$;

/**
 * 功能：事件时间属性 DataStream 转 Table 时定义
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/5/5 下午6:03
 */
public class EventTimeAttributeTableExample {
    public static void main(String[] args) throws Exception {
        // 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        //tEnv.getConfig().setLocalTimeZone(ZoneOffset.ofHours(8));

        tEnv.getConfig().setLocalTimeZone(ZoneId.of("UTC"));

        ZoneId localTimeZone = tEnv.getConfig().getLocalTimeZone();
        System.out.println(localTimeZone);

        DataStream<UserBehavior> sourceStream = env.fromElements(
                new UserBehavior(1001L, 3827899L, 2920476L, "pv", 1511713473000L, "2017-11-27 00:24:33"),
                new UserBehavior(1001L, 3745169L, 2891509L, "pv", 1511725471000L, "2017-11-27 03:44:31"),
                new UserBehavior(1001L, 1531036L, 2920476L, "pv", 1511733732000L, "2017-11-27 06:02:12"),
                new UserBehavior(1001L, 2266567L, 4145813L, "pv", 1511741471000L, "2017-11-27 08:11:11"),
                new UserBehavior(1001L, 2951368L, 1080785L, "pv", 1511750828000L, "2017-11-27 10:47:08"),
                new UserBehavior(1002L, 5002615L, 2520377L, "pv", 1511752985000L, "2017-11-27 11:23:05")
        );

        // 提取时间戳并分配 Watermark
        DataStream<UserBehavior> behaviorStream = sourceStream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<UserBehavior>forBoundedOutOfOrderness(Duration.ofMinutes(1))
                        .withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
                            @Override
                            public long extractTimestamp(UserBehavior behavior, long recordTimestamp) {
                                return behavior.getTs();
                            }
                        })
        );

        // 注册虚拟表
//        Table behaviorTable = tEnv.fromDataStream(behaviorStream, $("uid"), $("ts"), $("ts_row").rowtime());
//        tEnv.createTemporaryView("user_behavior", behaviorTable);

        tEnv.createTemporaryView("user_behavior", behaviorStream, $("uid"), $("ts"), $("ts_row").rowtime());



        Table resultTable = tEnv.sqlQuery("SELECT uid, ts, ts_row, DATE_FORMAT(ts_row, 'yyyy-MM-dd HH:mm:ss') FROM user_behavior");
        DataStream<Row> resultStream = tEnv.toAppendStream(resultTable, Row.class);
        resultStream.print();

        env.execute("EventTimeAttributeTableExample");
    }
}
