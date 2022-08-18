package com.flink.example.stream.sink;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.runtime.operators.CheckpointCommitter;
import org.apache.flink.streaming.runtime.operators.GenericWriteAheadSink;

/**
 * 功能：
 * 作者：SmartSi
 * CSDN博客：https://blog.csdn.net/sunnyyoona
 * 公众号：大数据生态
 * 日期：2022/8/18 上午8:46
 */
public class CustomWriteAheadSink extends GenericWriteAheadSink {

    public CustomWriteAheadSink(CheckpointCommitter committer, TypeSerializer serializer, String jobID) throws Exception {
        super(committer, serializer, jobID);
    }

    @Override
    protected boolean sendValues(Iterable values, long checkpointId, long timestamp) throws Exception {
        return false;
    }
}
