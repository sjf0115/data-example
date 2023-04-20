package com.flink.example.stream.function.stateful;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
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

        env.execute("ListCheckpointedExample");
    }

    // 自定义实现 ListCheckpointed
    public static class BufferingSink extends RichSinkFunction<String> implements ListCheckpointed<String> {
        private List<String> bufferedWords;
        private final int threshold;

        public BufferingSink(int threshold) {
            this.threshold = threshold;
            this.bufferedWords = new ArrayList<>();
        }

        @Override
        public void invoke(String word, Context context) throws Exception {
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
        public List snapshotState(long checkpointId, long timestamp) throws Exception {
            int subTask = getRuntimeContext().getIndexOfThisSubtask();
            LOG.info("snapshotState subTask: {}, checkpointId: {}, words: {}", subTask, checkpointId, bufferedWords.toString());
            // 无需清空上一次快照的状态 直接返回 List 即可
//            return Collections.singletonList(bufferedWords);
            return bufferedWords;
        }

        @Override
        public void restoreState(List<String> state) throws Exception {
            int subTask = getRuntimeContext().getIndexOfThisSubtask();
            // 不需要初始化 ListState
            // 从状态中恢复
            for (String word : state) {
                bufferedWords.add(word);
            }
            LOG.info("initializeState subTask: {}, words: {}", subTask, bufferedWords.toString());
        }
    }
}
