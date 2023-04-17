package com.flink.example.stream.function.base;

import com.flink.example.stream.function.stateful.BufferingSink;
import com.flink.example.stream.function.stateful.StatefulSinkExample;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * 功能：富函数示例 - 计算单词出现个数
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2023/4/16 下午11:02
 */
public class RichFunctionExample {
    private static final Logger LOG = LoggerFactory.getLogger(RichFunctionExample.class);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        // 每10s一次Checkpoint
        env.enableCheckpointing(30 * 1000);

        // Socket 输入
        DataStream<String> stream = env.socketTextStream("localhost", 9100, "\n");

        // 单词流
        DataStream<Tuple2<String, Long>> wordsStream = stream.flatMap(new FlatMapFunction<String, String>() {
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
        }).map(new WordCounterMapFunction()); // 使用富函数
        // 输出
        wordsStream.print();
        env.execute("RichFunctionExample");
    }

    // 自定义实现富函数 RichMapFunction
    private static class WordCounterMapFunction extends RichMapFunction<String, Tuple2<String, Long>> {
        private ValueState<Long> counterState;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            ValueStateDescriptor<Long> stateDescriptor = new ValueStateDescriptor<>("counter", Long.class);
            counterState = getRuntimeContext().getState(stateDescriptor);
        }

        @Override
        public Tuple2<String, Long> map(String word) throws Exception {
            int subtask = getRuntimeContext().getIndexOfThisSubtask();
            Long count = counterState.value();
            if (Objects.equals(count, null)) {
                count = 0L;
            }
            Long newCount = count + 1;
            counterState.update(newCount);
            LOG.info("word: {}, count: {}, subtask: {}", word, newCount, subtask);
            return new Tuple2<>(word, newCount);
        }

        @Override
        public void close() throws Exception {
            super.close();
        }
    }
}
