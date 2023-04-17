package com.flink.example.stream.function.stateful;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * 功能：实现带有缓冲区的 Sink
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2023/4/16 下午5:54
 */
public class BufferingSink extends RichSinkFunction<String> implements CheckpointedFunction {

    private static final Logger LOG = LoggerFactory.getLogger(BufferingSink.class);

    private final int threshold;
    private transient ListState<String> statePerPartition;
    private List<String> bufferedElements;

    public BufferingSink(int threshold) {
        this.threshold = threshold;
        this.bufferedElements = new ArrayList<>();
    }

    @Override
    public void invoke(String element, Context context) {
        int subTask = getRuntimeContext().getIndexOfThisSubtask();
        bufferedElements.add(element);
        LOG.info("buffer subTask: {}, element: {}, size: {}", subTask, element, bufferedElements.size());
        // 缓冲达到阈值输出
        if (bufferedElements.size() == threshold) {
            for (String bufferElement: bufferedElements) {
                // 输出
                LOG.info("buffer sink subTask: {}, element: {}", subTask, bufferElement);
            }
            bufferedElements.clear();
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        long checkpointId = context.getCheckpointId();
        long checkpointTimestamp = context.getCheckpointTimestamp();
        int subTask = getRuntimeContext().getIndexOfThisSubtask();
        statePerPartition.clear();
        for (String element : bufferedElements) {
            LOG.info("snapshotState subTask: {}, checkpointId: {}, checkpointTimestamp: {}, element: {}",
                    subTask, checkpointId, checkpointTimestamp, element);
            statePerPartition.add(element);
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        LOG.info("initializeState........");
        ListStateDescriptor<String> descriptor = new ListStateDescriptor<>(
                "buffered-elements",
                TypeInformation.of(new TypeHint<String>() {})
        );
        statePerPartition = context.getOperatorStateStore().getListState(descriptor);

        // 从状态中恢复
        if (context.isRestored()) {
            for (String element : statePerPartition.get()) {
                LOG.info("initializeState restored element: {}", element);
                bufferedElements.add(element);
            }
        }
    }
}
