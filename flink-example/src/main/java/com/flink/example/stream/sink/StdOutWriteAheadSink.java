package com.flink.example.stream.sink;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.runtime.operators.GenericWriteAheadSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

/**
 * 功能：StdOutWriteAheadSink
 * 作者：SmartSi
 * CSDN博客：https://blog.csdn.net/sunnyyoona
 * 公众号：大数据生态
 * 日期：2022/8/18 上午8:46
 */
public class StdOutWriteAheadSink extends GenericWriteAheadSink<Tuple2<String, Integer>> {

    private static final Logger LOG = LoggerFactory.getLogger(StdOutWriteAheadSink.class);

    // 构造函数
    public StdOutWriteAheadSink() throws Exception {
        super(
                new FileCheckpointCommitter(System.getProperty("java.io.tmpdir")),
                Types.<Tuple2<String, Integer>>TUPLE(Types.STRING, Types.INT).createSerializer(new ExecutionConfig()),
                UUID.randomUUID().toString()
        );
    }

    @Override
    protected boolean sendValues(Iterable<Tuple2<String, Integer>> values, long checkpointId, long timestamp) throws Exception {
        // 输出到外部系统 在这为 StdOut 标准输出
        // 每次 Checkpoint 完成之后通过 notifyCheckpointComplete 调用该方法
        LOG.info("[SendValues] CheckpointId: {}", checkpointId);
        for (Tuple2<String, Integer> tuple2 : values) {
            System.out.println("StdOut> " + tuple2);
        }
        return true;
    }
}
