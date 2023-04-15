package com.flink.example.stream.sink.wal;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.io.disk.InputViewIterator;
import org.apache.flink.runtime.state.*;
import org.apache.flink.runtime.util.ReusingMutableToRegularIteratorWrapper;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.operators.CheckpointCommitter;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;

/**
 * 功能：CustomGenericWriteAheadSink
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2023/4/15 上午8:45
 */
public abstract class CustomGenericWriteAheadSink<IN> extends AbstractStreamOperator<IN>
        implements OneInputStreamOperator<IN, IN> {

    private final String id;
    private final CheckpointCommitter committer;
    protected final TypeSerializer<IN> serializer;

    private transient CheckpointStorageWorkerView checkpointStorage;
    private final Set<PendingCheckpoint> pendingCheckpoints = new TreeSet<>();
    private transient ListState<PendingCheckpoint> checkpointedState;
    private transient CheckpointStreamFactory.CheckpointStateOutputStream out;

    //------------------------------------------------------------------------------------------------------------------
    // 1. 构造器

    public CustomGenericWriteAheadSink(CheckpointCommitter committer, TypeSerializer<IN> serializer, String jobID) throws Exception {
        this.committer = Preconditions.checkNotNull(committer);
        this.serializer = Preconditions.checkNotNull(serializer);
        this.id = UUID.randomUUID().toString();
        this.committer.setJobId(jobID);
        this.committer.createResource();
    }

    //------------------------------------------------------------------------------------------------------------------
    // 2.

    @Override
    public void open() throws Exception {
        super.open();
        committer.setOperatorId(id);
        committer.open();
        checkpointStorage = getContainingTask().getCheckpointStorage();
        cleanRestoredHandles();
    }

    @Override
    public void close() throws Exception {
        super.close();
        committer.close();
    }

    //------------------------------------------------------------------------------------------------------------------
    // 2. 状态处理

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);
        Preconditions.checkState(this.checkpointedState == null, "The reader state has already been initialized.");
        // 创建算子 Checkpoint 状态
        checkpointedState = context.getOperatorStateStore().getListState(
                new ListStateDescriptor<>("pending-checkpoints", new JavaSerializer<>())
        );
        // 子任务Id
        int subtaskIdx = getRuntimeContext().getIndexOfThisSubtask();
        // 是否需要从状态中恢复
        if (context.isRestored()) {
            LOG.info("Restoring state for the GenericWriteAheadSink (taskIdx={}).", subtaskIdx);
            for (PendingCheckpoint pendingCheckpoint : checkpointedState.get()) {
                this.pendingCheckpoints.add(pendingCheckpoint);
            }
        } else {
            LOG.info("No state to restore for the GenericWriteAheadSink (taskIdx={}).", subtaskIdx);
        }
    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);
        Preconditions.checkState(this.checkpointedState != null, "The operator state has not been properly initialized.");

        saveHandleInState(context.getCheckpointId(), context.getCheckpointTimestamp());

        this.checkpointedState.clear();

        try {
            for (PendingCheckpoint pendingCheckpoint : pendingCheckpoints) {
                this.checkpointedState.add(pendingCheckpoint);
            }
        } catch (Exception e) {
            checkpointedState.clear();
            throw new Exception(
                    "Could not add panding checkpoints to operator state backend of operator " + getOperatorName() + '.',
                    e
            );
        }
    }

    //------------------------------------------------------------------------------------------------------------------
    // 3. Checkpoint

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        super.notifyCheckpointComplete(checkpointId);
        synchronized (pendingCheckpoints) {
            Iterator<PendingCheckpoint> pendingCheckpointIt = pendingCheckpoints.iterator();
            // 处理每个 PendingCheckpoint
            while (pendingCheckpointIt.hasNext()) {
                PendingCheckpoint pendingCheckpoint = pendingCheckpointIt.next();
                long pastCheckpointId = pendingCheckpoint.checkpointId;
                int subtaskId = pendingCheckpoint.subtaskId;
                long timestamp = pendingCheckpoint.timestamp;
                StreamStateHandle streamHandle = pendingCheckpoint.stateHandle;
                // 处理小于当前 checkpointId 的 Checkpoint
                if (pastCheckpointId <= checkpointId) {
                    try {
                        // 是否已提交
                        if (!committer.isCheckpointCommitted(subtaskId, pastCheckpointId)) {
                            // 未提交
                            try (FSDataInputStream in = streamHandle.openInputStream()) {
                                // 判断是否发送成功
                                ReusingMutableToRegularIteratorWrapper<IN> ins = new ReusingMutableToRegularIteratorWrapper<>(
                                        new InputViewIterator<>(new DataInputViewStreamWrapper(in), serializer),
                                        serializer
                                );
                                boolean success = sendValues(ins, pastCheckpointId, timestamp);
                                // 发送成功
                                if (success) {
                                    committer.commitCheckpoint(subtaskId, pastCheckpointId);
                                    streamHandle.discardState();
                                    pendingCheckpointIt.remove();
                                }
                            }
                        } else {
                            // 已提交
                            streamHandle.discardState();
                            pendingCheckpointIt.remove();
                        }
                    } catch (Exception e) {
                        LOG.error("Could not commit checkpoint.", e);
                        break;
                    }
                }
            }
        }
    }

    protected abstract boolean sendValues(Iterable<IN> values, long checkpointId, long timestamp) throws Exception;

    //------------------------------------------------------------------------------------------------------------------
    // 4. 元素处理

    @Override
    public void processElement(StreamRecord<IN> element) throws Exception {
        IN value = element.getValue();
        if (out == null) {
            out = checkpointStorage.createTaskOwnedStateStream();
        }
        serializer.serialize(value, new DataOutputViewStreamWrapper(out));
    }

    //------------------------------------------------------------------------------------------------------------------
    // 5. 私有方法

    // Checkpoint 的封装
    private static final class PendingCheckpoint implements Comparable<PendingCheckpoint>, Serializable {
        private static final long serialVersionUID = -3571036395734603443L;
        private final long checkpointId;
        private final int subtaskId;
        private final long timestamp;
        private final StreamStateHandle stateHandle;

        PendingCheckpoint(long checkpointId, int subtaskId, long timestamp, StreamStateHandle handle) {
            this.checkpointId = checkpointId;
            this.subtaskId = subtaskId;
            this.timestamp = timestamp;
            this.stateHandle = handle;
        }

        @Override
        public int compareTo(PendingCheckpoint o) {
            int res = Long.compare(this.checkpointId, o.checkpointId);
            return res != 0 ? res : this.subtaskId - o.subtaskId;
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof PendingCheckpoint)) {
                return false;
            }
            PendingCheckpoint other = (PendingCheckpoint) o;
            return this.checkpointId == other.checkpointId
                    && this.subtaskId == other.subtaskId
                    && this.timestamp == other.timestamp;
        }

        @Override
        public int hashCode() {
            int hash = 17;
            hash = 31 * hash + (int) (checkpointId ^ (checkpointId >>> 32));
            hash = 31 * hash + subtaskId;
            hash = 31 * hash + (int) (timestamp ^ (timestamp >>> 32));
            return hash;
        }

        @Override
        public String toString() {
            return "Pending Checkpoint: id=" + checkpointId + "/" + subtaskId + "@" + timestamp;
        }
    }

    private void cleanRestoredHandles() throws Exception {
        synchronized (pendingCheckpoints) {
            Iterator<PendingCheckpoint> pendingCheckpointIt = pendingCheckpoints.iterator();
            while (pendingCheckpointIt.hasNext()) {
                PendingCheckpoint pendingCheckpoint = pendingCheckpointIt.next();
                if (committer.isCheckpointCommitted(pendingCheckpoint.subtaskId, pendingCheckpoint.checkpointId)) {
                    pendingCheckpoint.stateHandle.discardState();
                    pendingCheckpointIt.remove();
                }
            }
        }
    }

    private void saveHandleInState(final long checkpointId, final long timestamp) throws Exception {
        if (out != null) {
            int subtaskIdx = getRuntimeContext().getIndexOfThisSubtask();
            StreamStateHandle handle = out.closeAndGetHandle();

            PendingCheckpoint pendingCheckpoint = new PendingCheckpoint(checkpointId, subtaskIdx, timestamp, handle);

            if (pendingCheckpoints.contains(pendingCheckpoint)) {
                handle.discardState();
            } else {
                pendingCheckpoints.add(pendingCheckpoint);
            }
            out = null;
        }
    }
}
