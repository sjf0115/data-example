package com.flink.example.stream.connector.kafka;

import org.apache.flink.annotation.VisibleForTesting;
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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.config.OffsetCommitMode;
import org.apache.flink.streaming.connectors.kafka.config.OffsetCommitModes;
import org.apache.flink.streaming.connectors.kafka.internals.AbstractFetcher;
import org.apache.flink.streaming.connectors.kafka.internals.AbstractPartitionDiscoverer;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicsDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.TreeMap;
import java.util.regex.Pattern;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * 功能：自定义 KafkaConsumerBase
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/3/8 下午10:16
 */
public abstract class CustomKafkaConsumerBase<T> extends RichParallelSourceFunction<T>
        implements ResultTypeQueryable<T>, CheckpointListener, CheckpointedFunction {

    protected static final Logger LOG = LoggerFactory.getLogger(CustomKafkaConsumerBase.class);

    // consumer 是否运行的标志
    private volatile boolean running = true;
    protected final KafkaDeserializationSchema<T> deserializer;

    // 分区偏移量 OperatorState
    private transient ListState<Tuple2<KafkaTopicPartition, Long>> unionOffsetStates;
    // 分区偏移量 OperatorState 名称
    private static final String OFFSETS_STATE_NAME = "topic-partition-offset-states";
    // 恢复状态
    private transient volatile TreeMap<KafkaTopicPartition, Long> restoredState;

    // Offset 提交模式
    private OffsetCommitMode offsetCommitMode;
    // Checkpoint 是是否提交 Offset
    private boolean enableCommitOnCheckpoints = true;

    // 分区发现器 用来发现新的分区
    private transient volatile AbstractPartitionDiscoverer partitionDiscoverer;
    /** Describes whether we are discovering partitions for fixed topics or a topic pattern. */
    private final KafkaTopicsDescriptor topicsDescriptor;

    // 实现连接 Kafka Broker 的逻辑
    private transient volatile AbstractFetcher<T, ?> kafkaFetcher;

    public CustomKafkaConsumerBase(
            List<String> topics,
            Pattern topicPattern,
            KafkaDeserializationSchema<T> deserializer,
            long discoveryIntervalMillis,
            boolean useMetrics){
        this.topicsDescriptor = new KafkaTopicsDescriptor(topics, topicPattern);
        this.deserializer = checkNotNull(deserializer, "valueDeserializer");
//        checkArgument(discoveryIntervalMillis == PARTITION_DISCOVERY_DISABLED || discoveryIntervalMillis >= 0, "Cannot define a negative value for the topic / partition discovery interval.");
//        this.discoveryIntervalMillis = discoveryIntervalMillis;
//        this.useMetrics = useMetrics;
    }

    // 1. ResultTypeQueryable
    @Override
    public TypeInformation<T> getProducedType() {
        // 返回值数据类型信息
        return deserializer.getProducedType();
    }

    // 2. RichParallelSourceFunction
    @Override
    public void open(Configuration parameters) throws Exception {
        // 确定 Offset 提交模式
        this.offsetCommitMode = OffsetCommitModes.fromConfiguration(
                getIsAutoCommitEnabled(),
                enableCommitOnCheckpoints,
                ((StreamingRuntimeContext) getRuntimeContext()).isCheckpointingEnabled()
        );

        // 分区发现器
        this.partitionDiscoverer = createPartitionDiscoverer(
                topicsDescriptor,
                getRuntimeContext().getIndexOfThisSubtask(),
                getRuntimeContext().getNumberOfParallelSubtasks()
        );
        this.partitionDiscoverer.open();

    }

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
        if (!running) {
            LOG.debug("snapshotState() called on closed source");
        } else {

        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        // 分区偏移量 OperatorState
        OperatorStateStore stateStore = context.getOperatorStateStore();
        TupleSerializer<Tuple2<KafkaTopicPartition, Long>> stateSerializer = createStateSerializer(getRuntimeContext().getExecutionConfig());
        ListStateDescriptor<Tuple2<KafkaTopicPartition, Long>> stateDescriptor = new ListStateDescriptor<>(OFFSETS_STATE_NAME, stateSerializer);
        this.unionOffsetStates = stateStore.getUnionListState(stateDescriptor);

        int subTask = getRuntimeContext().getIndexOfThisSubtask();
        if (context.isRestored()) {
            // 状态恢复
            restoredState = new TreeMap<>(new KafkaTopicPartition.Comparator());
            for (Tuple2<KafkaTopicPartition, Long> kafkaOffset : unionOffsetStates.get()) {
                restoredState.put(kafkaOffset.f0, kafkaOffset.f1);
            }
            LOG.info("Consumer subTask {} restored state: {}.", subTask, restoredState);
        } else {
            LOG.info("Consumer subTask {} has no restore state.", subTask);
        }
    }

    //------------------------------------------------------------------------------------------------------------------

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

    protected abstract boolean getIsAutoCommitEnabled();

    @VisibleForTesting
    public boolean getEnableCommitOnCheckpoints() {
        return enableCommitOnCheckpoints;
    }

    // 创建分区发现器
    protected abstract AbstractPartitionDiscoverer createPartitionDiscoverer(KafkaTopicsDescriptor topicsDescriptor, int indexOfThisSubtask, int numParallelSubtasks);
}
