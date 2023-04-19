package com.flink.example.stream.function.stateful;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * 功能：CheckpointedFunction 操作 OperatorState 示例
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2023/4/17 下午11:03
 */
public class CheckpointedFunctionOSExample2 {
    private static final Logger LOG = LoggerFactory.getLogger(CheckpointedFunctionOSExample2.class);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        // 每10s一次Checkpoint
        env.enableCheckpointing(30 * 1000);

        // Socket 输入
        DataStream<String> stream = env.socketTextStream("localhost", 9100, "\n");

        // 单词流
        DataStream<Tuple2<String, Long>> wordStream = stream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector out) {
                for (String word : value.split("\\s")) {
                    LOG.info("word: {}", word);
                    out.collect(word);
                }
            }
        }).keyBy(new KeySelector<String, String>() {
            @Override
            public String getKey(String word) throws Exception {
                return word;
            }
        }).map(new CounterMapFunction());
        wordStream.print();

        env.execute("CheckpointedFunctionOperatorStateExample");
    }

    // 自定义实现 CheckpointedFunction 操作 OperatorState
    public static class CounterMapFunction extends RichMapFunction<String, Tuple2<String, Long>> implements CheckpointedFunction {
        // 算子状态计数器
        private ListState<Long> countPerPartition;
        // 本地计数器
        private long count;

        @Override
        public Tuple2<String, Long> map(String word) throws Exception {
            count++;
            LOG.info("word: {}, count: {}", word, count);
            return Tuple2.of(word, count);
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            countPerPartition = context.getOperatorStateStore().getListState(
                    new ListStateDescriptor<>("perPartitionCount", Long.class));
            if (context.isRestored()) {
                for (Long count : countPerPartition.get()) {
                    this.count += count;
                }
            }
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            countPerPartition.clear();
            countPerPartition.add(count);
        }
    }
}
