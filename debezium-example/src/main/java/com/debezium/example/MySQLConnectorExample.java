package com.debezium.example;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * 功能：MySQL Connector 示例
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2021/8/14 上午11:08
 */
public class MySQLConnectorExample {
    public static void main(String[] args) {
        // Define the configuration for the Debezium Engine with MySQL connector...
        final Properties props = config.asProperties();
        // 引擎配置
        props.setProperty("name", "engine");
        props.setProperty("connector.class", "io.debezium.connector.mysql.MySqlConnector");
        props.setProperty("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore");
        props.setProperty("offset.storage.file.filename", "/tmp/offsets.dat");
        props.setProperty("offset.flush.interval.ms", "60000");
        // Connector 配置
        props.setProperty("database.hostname", "localhost");
        props.setProperty("database.port", "3306");
        props.setProperty("database.user", "root");
        props.setProperty("database.password", "root");
        props.setProperty("database.server.id", "85744");
        props.setProperty("database.server.name", "my-app-connector");
        props.setProperty("database.history", "io.debezium.relational.history.FileDatabaseHistory");
        props.setProperty("database.history.file.filename", "/path/to/storage/dbhistory.dat");

        // 创建引擎
        DebeziumEngine.Builder<ChangeEvent<String, String>> builder = DebeziumEngine.create(Json.class);
        builder.using(props);
        builder.notifying(record -> {
            System.out.println(record);
        });
        DebeziumEngine<ChangeEvent<String, String>> engine = builder.build();

        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.execute(engine);

        try {
            executor.shutdown();
            while (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                System.out.println("Waiting another 5 seconds for the embedded engine to shut down");
            }
        } catch ( InterruptedException e ) {
            Thread.currentThread().interrupt();
        }
    }
}
