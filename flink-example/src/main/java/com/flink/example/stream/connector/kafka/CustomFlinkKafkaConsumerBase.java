package com.flink.example.stream.connector.kafka;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;

import java.util.TreeMap;

/**
 * 功能：自定义 FlinkKafkaConsumerBase
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/3/8 下午10:16
 */
public class CustomFlinkKafkaConsumerBase<T> extends RichParallelSourceFunction<T>
        implements ResultTypeQueryable<T>, CheckpointListener, CheckpointedFunction {

    protected final KafkaDeserializationSchema<T> deserializer;

    // 分区偏移量 OperatorState
    private transient ListState<Tuple2<KafkaTopicPartition, Long>> unionOffsetStates;
    // 分区偏移量 OperatorState 名称
    private static final String OFFSETS_STATE_NAME = "topic-partition-offset-states";
    // 恢复状态
    private transient volatile TreeMap<KafkaTopicPartition, Long> restoredState;

    public CustomFlinkKafkaConsumerBase(KafkaDeserializationSchema<T> deserializer) {
        this.deserializer = deserializer;
    }

    // 1. ResultTypeQueryable
    @Override
    public TypeInformation<T> getProducedType() {
        // 返回值数据类型信息
        return deserializer.getProducedType();
    }

    // 2. RichParallelSourceFunction

    @Override
    public void run(SourceContext<T> ctx) throws Exception {

    }

    @Override
    public void cancel() {

    }

    // 3. CheckpointListener
    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {

    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) throws Exception {

    }

    // 4. CheckpointedFunction
    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {

    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        // 分区偏移量 OperatorState
        OperatorStateStore stateStore = context.getOperatorStateStore();
        TupleSerializer<Tuple2<KafkaTopicPartition, Long>> stateSerializer = createStateSerializer(getRuntimeContext().getExecutionConfig());
        ListStateDescriptor<Tuple2<KafkaTopicPartition, Long>> stateDescriptor = new ListStateDescriptor<>(OFFSETS_STATE_NAME, stateSerializer);
        this.unionOffsetStates = stateStore.getUnionListState(stateDescriptor);

        // 状态恢复
        if (context.isRestored()) {
            // 状态恢复
            restoredState = new TreeMap<>(new KafkaTopicPartition.Comparator());
            for (Tuple2<KafkaTopicPartition, Long> kafkaOffset : unionOffsetStates.get()) {
                restoredState.put(kafkaOffset.f0, kafkaOffset.f1);
            }
        }
    }

    // 状态序列化器
    static TupleSerializer<Tuple2<KafkaTopicPartition, Long>> createStateSerializer(
            ExecutionConfig executionConfig) {
        // explicit serializer will keep the compatibility with GenericTypeInformation and allow to
        // disableGenericTypes for users
        TypeSerializer<?>[] fieldSerializers =
                new TypeSerializer<?>[] {
                        new KryoSerializer<>(KafkaTopicPartition.class, executionConfig),
                        LongSerializer.INSTANCE
                };
        @SuppressWarnings("unchecked")
        Class<Tuple2<KafkaTopicPartition, Long>> tupleClass =
                (Class<Tuple2<KafkaTopicPartition, Long>>) (Class<?>) Tuple2.class;
        return new TupleSerializer<>(tupleClass, fieldSerializers);
    }
}
