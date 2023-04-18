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
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;

/**
 * 功能：ListCheckpointed 示例
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2023/4/18 上午8:00
 */
public class ListCheckpointedExample {
    private static final Logger LOG = LoggerFactory.getLogger(ListCheckpointedExample.class);

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

        env.execute("ListCheckpointedExample");
    }

    // 自定义实现 ListCheckpointed
    public static class CounterMapFunction extends RichMapFunction<String, Tuple2<String, Long>> implements ListCheckpointed<Long> {
        private long localCount;

        @Override
        public Tuple2<String, Long> map(String word) throws Exception {
            localCount++;
            LOG.info("word: {}, count: {}", word, localCount);
            return Tuple2.of(word, localCount);
        }

        @Override
        public List snapshotState(long checkpointId, long timestamp) throws Exception {
            return Collections.singletonList(localCount);
        }

        @Override
        public void restoreState(List<Long> state) throws Exception {
            for (Long count : state) {
                localCount += count;
            }
        }
    }
}
