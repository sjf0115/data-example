package com.flink.example.stream.function.stateful;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * 功能：有状态 Sink 示例
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2023/4/16 下午6:14
 */
public class CheckpointedFunctionOSExample {
    private static final Logger LOG = LoggerFactory.getLogger(CheckpointedFunctionOSExample.class);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        // 每10s一次Checkpoint
        env.enableCheckpointing(30 * 1000);
        // 重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                3, // 重启最大次数
                Time.of(10, TimeUnit.SECONDS) // 重启时间间隔
        ));

        // Socket 输入
        DataStream<String> stream = env.socketTextStream("localhost", 9100, "\n");

        // 单词流
        DataStream<String> wordStream = stream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector out) {
                for (String word : value.split("\\s")) {
                    LOG.info("word: {}", word);
                    if (Objects.equals(word, "ERROR")) {
                        throw new RuntimeException("error dirty data");
                    }
                    out.collect(word);
                }
            }
        }).keyBy(new KeySelector<String, String>() {
            @Override
            public String getKey(String word) throws Exception {
                return word;
            }
        });

        // 每个并行实例缓冲4个单词输出一次
        wordStream.addSink(new BufferingSink(4));

        env.execute("CheckpointedFunctionOperatorStateExample");
    }

    public static class BufferingSink extends RichSinkFunction<String> implements CheckpointedFunction {

        private static final Logger LOG = LoggerFactory.getLogger(CheckpointedFunctionOSExample.class);

        private final int threshold;
        private transient ListState<String> statePerPartition;
        private List<String> bufferedWords;

        public BufferingSink(int threshold) {
            this.threshold = threshold;
            this.bufferedWords = new ArrayList<>();
        }

        @Override
        public void invoke(String word, Context context) {
            int subTask = getRuntimeContext().getIndexOfThisSubtask();
            bufferedWords.add(word);
            LOG.info("invoke buffer subTask: {}, words: {}", subTask, bufferedWords.toString());
            // 缓冲达到阈值输出
            if (bufferedWords.size() == threshold) {
                for (String bufferedWord: bufferedWords) {
                    // 输出
                    LOG.info("invoke sink subTask: {}, word: {}", subTask, bufferedWord);
                }
                bufferedWords.clear();
            }
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            long checkpointId = context.getCheckpointId();
            int subTask = getRuntimeContext().getIndexOfThisSubtask();
            // 清空上一次快照的状态
            statePerPartition.clear();
            // 生成新快照的状态
            for (String word : bufferedWords) {
                statePerPartition.add(word);
            }
            LOG.info("snapshotState subTask: {}, checkpointId: {}, words: {}", subTask, checkpointId, bufferedWords.toString());
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            ListStateDescriptor<String> descriptor = new ListStateDescriptor<>(
                    "buffered-words", TypeInformation.of(new TypeHint<String>() {}));
            statePerPartition = context.getOperatorStateStore().getListState(descriptor);
            int subTask = getRuntimeContext().getIndexOfThisSubtask();
            // 从状态中恢复
            if (context.isRestored()) {
                for (String word : statePerPartition.get()) {
                    bufferedWords.add(word);
                }
                LOG.info("initializeState subTask: {}, words: {}", subTask, bufferedWords.toString());
            }
        }
    }
}
