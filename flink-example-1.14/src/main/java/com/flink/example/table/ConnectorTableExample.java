package com.flink.example.table;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * 功能：
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/4/3 下午4:31
 */
public class ConnectorTableExample {
    public static void main(String[] args) {

        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();

        TableEnvironment tableEnv = TableEnvironment.create(settings);

//        final TableDescriptor sourceDescriptor = TableDescriptor.forConnector("datagen")
//                .schema(Schema.newBuilder()
//                        .column("f0", DataTypes.STRING())
//                        .build())
//                .option(DataGenConnectorOptions.NUMBER_OF_ROWS, "1")
//                .build();
//
//        tableEnv.createTable("SourceTableA", sourceDescriptor);
//        tableEnv.createTemporaryTable("SourceTableB", sourceDescriptor);
    }
}
