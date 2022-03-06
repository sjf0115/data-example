package com.flink.example.stream.source;

import org.apache.flink.api.common.io.FilePathFilter;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ContinuousFileMonitoringFunction;
import org.apache.flink.streaming.api.functions.source.ContinuousFileReaderOperatorFactory;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.streaming.api.functions.source.TimestampedFileInputSplit;

/**
 * 功能：ContinuousFileMonitoringFunction 示例
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/3/6 下午7:21
 */
public class FileMonitoringExample {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        String path = "/Users/wy/test.txt";

        // 方式1
        DataStreamSource<String> source = env.readTextFile(path);
        source.print("1");

        // 方式2
        TextInputFormat inputFormat = new TextInputFormat(new Path(path));
        inputFormat.setFilesFilter(FilePathFilter.createDefaultFilter());
        inputFormat.setCharsetName("UTF-8");
        inputFormat.setFilePath(path);
        FileProcessingMode monitoringMode = FileProcessingMode.PROCESS_ONCE;

        ContinuousFileMonitoringFunction<String> function = new ContinuousFileMonitoringFunction<>(
                inputFormat,
                monitoringMode,
                env.getParallelism(),
                -1
        );

        ContinuousFileReaderOperatorFactory<String, TimestampedFileInputSplit> factory =
                new ContinuousFileReaderOperatorFactory<>(inputFormat);
        String sourceName = "FileMonitoring";
        TypeInformation<String> typeInfo = BasicTypeInfo.STRING_TYPE_INFO;

        SingleOutputStreamOperator<String> source1 = env.addSource(function, sourceName)
                .transform("Split Reader: " + sourceName, typeInfo, factory);
        source1.print("2");

        // 执行
        env.execute("FileMonitoringExample");
    }
}
