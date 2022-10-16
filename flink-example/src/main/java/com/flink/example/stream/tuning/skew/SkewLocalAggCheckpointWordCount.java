package com.flink.example.stream.tuning.skew;

import com.flink.example.stream.source.simple.SkewWordMockSource;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * 功能：数据倾斜版本 WordCount 实现本地聚合优化 可故障恢复
 *      Source 每一秒输出一个单词
 *      每个批次积攒20个元素
 *      每10s进行一次 Checkpoint
 *      假设
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2022/10/14 下午11:30
 */
public class SkewLocalAggCheckpointWordCount {
    private static final Logger LOG = LoggerFactory.getLogger(SkewLocalAggCheckpointWordCount.class);

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        // 设置状态后端
        env.setStateBackend(new HashMapStateBackend());

        // 设置Checkpoint存储
        env.enableCheckpointing(10000L);
        String checkpointPath = "hdfs://localhost:9000/flink/checkpoint";
        env.getCheckpointConfig().setCheckpointStorage(new FileSystemCheckpointStorage(checkpointPath));

        // 单词流 (word, 1)
        DataStreamSource<Tuple2<String, Integer>> source = env.addSource(new SkewWordMockSource(1, 40));

        // 统计单词出现个数
        DataStream<Tuple2<String, Integer>> windowCount = source
                // 实现本地攒批聚合 每20个数据记录一个批次
                .process(new LocalAggProcessFunction(20))
                // 分组
                .keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Integer> tuple) throws Exception {
                        return tuple.f0;
                    }
                })
                // 求和
                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2 reduce(Tuple2<String, Integer> a, Tuple2<String, Integer> b) {
                        return new Tuple2(a.f0, a.f1 + b.f1);
                    }
                });

        // 输出
        windowCount.print();
        env.execute("SkewLocalAggCheckpointWordCount");
    }

    // 本地攒批聚合 实现故障容错
    private static class LocalAggProcessFunction extends ProcessFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>
            implements CheckpointedFunction {
        // 为了保证 Exactly-Once 语义 需要将批次中缓存中的数据记录元素在 Checkpoint 保存在状态中
        private ListState<Tuple2<String, Integer>> bundleListState;
        // 批次大小
        private int batchSize = 10;
        // 本地批次
        private transient Map<String, Integer> bundle;
        // 数据记录个数
        private transient int elements = 0;

        public LocalAggProcessFunction() {
        }

        public LocalAggProcessFunction(int batchSize) {
            this.batchSize = batchSize;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            this.bundle = new HashMap<>();
        }

        @Override
        public void processElement(Tuple2<String, Integer> element, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
            // 1. 将新到达的数据记录添加到批次中
            String word = element.f0;
            if (Objects.equals(word, "error")) {
                throw new RuntimeException("模拟脏数据出现异常");
            }
            int count = element.f1;
            Integer bundleCount = bundle.getOrDefault(word, 0);
            bundle.put(word, bundleCount + count);

            LOG.info("process element {}, batch index is {}", element, elements);
            // 2. 当批次中的数据记录数达到批次大小则输出
            if (elements >= batchSize) {
                LOG.info("num of elements greater than or equal {}, output the batch", batchSize);
                // 依次输出批次中的数据记录
                for (String bundleKey : bundle.keySet()) {
                    out.collect(Tuple2.of(bundleKey, bundle.get(bundleKey)));
                }
                // 批次清空
                bundle.clear();
                elements = 0;
            }
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            // 清空状态 重新生成快照状态
            bundleListState.clear();
            // 添加到状态中
            for (String bundleKey : bundle.keySet()) {
                bundleListState.add(Tuple2.of(bundleKey, bundle.get(bundleKey)));
            }
            LOG.info("store bundle into list state, bundle have {} elements", elements);
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            ListStateDescriptor<Tuple2<String, Integer>> bundleListStateDescriptor = new ListStateDescriptor<>(
                    "BundleListState", Types.TUPLE(Types.STRING, Types.INT)
            );
            bundleListState = context.getOperatorStateStore().getListState(bundleListStateDescriptor);
            // 从状态中恢复
            if (context.isRestored()) {
                for (Tuple2<String, Integer> element : bundleListState.get()) {
                    String word = element.f0;
                    int count = element.f1;
                    Integer bundleCount = bundle.getOrDefault(word, 0);
                    bundle.put(word, bundleCount + count);
                }
                elements = bundle.size();
                LOG.info("restore from list state, bundle have {} elements", elements);
            } else {
                elements = 0;
            }
        }
    }
}
