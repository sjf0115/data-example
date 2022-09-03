package com.flink.example.stream.sink.print;

import org.apache.flink.api.common.functions.util.PrintSinkOutputWriter;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 功能：自定义实现打印日志的 Print Sink
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2022/9/3 下午3:06
 */
public class PrintLogSinkFunction<IN> extends RichSinkFunction<IN> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(PrintLogSinkFunction.class);

    private final PrintSinkOutputWriter<IN> writer;

    // 实例化 PrintSinkOutputWriter
    public PrintLogSinkFunction() {
        writer = new PrintSinkOutputWriter<>(false);
    }

    public PrintLogSinkFunction(final String sinkIdentifier) {
        writer = new PrintSinkOutputWriter<>(sinkIdentifier, false);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        StreamingRuntimeContext context = (StreamingRuntimeContext) getRuntimeContext();
        writer.open(context.getIndexOfThisSubtask(), context.getNumberOfParallelSubtasks());
    }

    @Override
    public void invoke(IN record) {
        LOG.info("{}", record);
        writer.write(record);
    }

    @Override
    public String toString() {
        return writer.toString();
    }
}
