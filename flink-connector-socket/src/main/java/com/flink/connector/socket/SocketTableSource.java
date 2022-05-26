package com.flink.connector.socket;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.types.Row;

/**
 * 功能：Socket TableSource
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/5/26 下午10:57
 */
public class SocketTableSource implements StreamTableSource<Row>{
    @Override
    public DataStream<Row> getDataStream(StreamExecutionEnvironment execEnv) {
        return null;
    }

    @Override
    public TableSchema getTableSchema() {
        return null;
    }
}
