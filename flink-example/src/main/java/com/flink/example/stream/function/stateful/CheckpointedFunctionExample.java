package com.flink.example.stream.function.stateful;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
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
 * 功能：CheckpointedFunction 示例
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2023/4/17 下午11:03
 */
public class CheckpointedFunctionExample {
    private static final Logger LOG = LoggerFactory.getLogger(CheckpointedFunctionExample.class);

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

        env.execute("CheckpointedFunctionExample");
    }

    // 自定义实现 CheckpointedFunction
    public static class CounterMapFunction extends RichMapFunction<String, Tuple2<String, Long>> implements CheckpointedFunction {
        private ReducingState<Long> countPerKey;
        private ListState<Long> countPerPartition;
        private long localCount;

        @Override
        public Tuple2<String, Long> map(String word) throws Exception {
            countPerKey.add(1L);
            localCount++;
            LOG.info("word: {}, count: {}", word, localCount);
            return Tuple2.of(word, localCount);
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            // 每个键的计数器
            countPerKey = context.getKeyedStateStore().getReducingState(
                    new ReducingStateDescriptor<>("perKeyCount", new ReduceFunction<Long>() {
                        @Override
                        public Long reduce(Long value1, Long value2) throws Exception {
                            return value1 + value2;
                        }
                    }, Long.class));
            // 每个实例的计数器
            countPerPartition = context.getOperatorStateStore().getListState(
                    new ListStateDescriptor<>("perPartitionCount", Long.class));

            if (context.isRestored()) {
                for (Long count : countPerPartition.get()) {
                    localCount += count;
                }
            }
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            countPerPartition.clear();
            countPerPartition.add(localCount);
        }
    }
}
