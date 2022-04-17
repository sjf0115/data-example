package com.flink.example.table.table;

import com.common.example.bean.User;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.scala.typeutils.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.Instant;

/**
 * 功能：Stream 转 Table
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/4/17 下午4:14
 */
public class TableToStreamExample {
    public static void main(String[] args) throws Exception {
        // Stream 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // Table 执行环境
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 读取数据创建 DataStream
        DataStream<User> dataStream =
                env.fromElements(
                        new User("Alice", 4, Instant.ofEpochMilli(1000)),
                        new User("Bob", 6, Instant.ofEpochMilli(1001)),
                        new User("Alice", 10, Instant.ofEpochMilli(1002))
                );

        // DataStream 转 Table
        Table table = tEnv.fromDataStream(dataStream);

//        DataStream<User> stream1 = tEnv.toAppendStream(table, User.class);
//        stream1.print("R1");
//
//        DataStream<User> stream2 = tEnv.toAppendStream(table, Types.POJO(User.class));
//        stream2.print("R2");

        DataStream<Row> stream3 = tEnv.toAppendStream(table,
                Types.ROW(
                        new String[]{"name", "score", "eventTime"},
                        new TypeInformation<?>[]{Types.STRING(), Types.INT(), Types.INSTANT()}
                )
        );
        stream3.print("R3");

        env.execute();
    }
}
