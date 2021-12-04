package com.flink.example.stream.sink;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.util.concurrent.TimeUnit;

/**
 * 功能：File Sink RowFormat
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2021/11/21 下午8:38
 */
public class RowFormatFileSinkExample {
    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> source = env.socketTextStream("localhost", 9100, "\n");

        String outputPath = "hdfs://localhost:9000/flink/file";
        final FileSink<String> sink = FileSink
                .forRowFormat(new Path(outputPath), new SimpleStringEncoder<String>("UTF-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                // 包含至少 15 分钟的数据
                                .withRolloverInterval(TimeUnit.MINUTES.toMillis(15))
                                // 最近 5 分钟没有收到新记录
                                .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
                                // 文件大小已达到 1 GB
                                .withMaxPartSize(1024 * 1024 * 1024)
                                .build()
                )
                .build();

        source.sinkTo(sink);

        env.execute("RowFormatFileSinkExample");
    }
}
