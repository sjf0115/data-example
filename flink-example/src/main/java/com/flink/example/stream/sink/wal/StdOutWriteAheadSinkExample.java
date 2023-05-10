package com.flink.example.stream.sink.wal;

import com.common.example.bean.WordCount;
import com.flink.example.stream.state.checkpoint.FileCheckpointCommitter;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.runtime.operators.GenericWriteAheadSink;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * 功能：StdOutWriteAheadSink 示例
 * 作者：SmartSi
 * CSDN博客：https://blog.csdn.net/sunnyyoona
 * 公众号：大数据生态
 * 日期：2022/8/20 下午3:29
 */
public class StdOutWriteAheadSinkExample {
    private static Gson gson = new GsonBuilder().create();
    private static final Logger LOG = LoggerFactory.getLogger(StdOutWriteAheadSinkExample.class);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        // 每隔 30s 进行一次 Checkpoint 如果不设置 Checkpoint 自定义 WAL Sink 不会输出数据
        env.enableCheckpointing(30 * 1000);
        // 重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                3, // 重启最大次数
                Time.of(10, TimeUnit.SECONDS) // 重启时间间隔
        ));

        // 创建 Kafka Consumer
        Properties consumerProps = new Properties();
        consumerProps.put("bootstrap.servers", "localhost:9092");
        consumerProps.put("group.id", "word-count");
        String consumerTopic = "word";
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(consumerTopic, new SimpleStringSchema(), consumerProps);
        consumer.setStartFromLatest();
        DataStreamSource<String> stream = env.addSource(consumer);

        // 单词流
        DataStream<Tuple2<String, Long>> result = stream.flatMap(new RichFlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String value, Collector out) {
                int subtask = getRuntimeContext().getIndexOfThisSubtask();
                WordCount wc = gson.fromJson(value, WordCount.class);
                LOG.info("subTask {} input word: {}", subtask, wc.getWord());
                // 模拟程序 Failover 遇到 error 抛出异常
                if (Objects.equals(wc.getWord(), "ERROR")) {
                    throw new RuntimeException("模拟程序 Failover");
                }
                out.collect(Tuple2.of(wc.getWord(), wc.getFrequency()));
            }
        }).keyBy(new KeySelector<Tuple2<String, Long>, String>() {
            @Override
            public String getKey(Tuple2<String, Long> wc) throws Exception {
                return wc.f0;
            }
        }).reduce(new ReduceFunction<Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> reduce(Tuple2<String, Long> wc1, Tuple2<String, Long> wc2) throws Exception {
                return Tuple2.of(wc1.f0, wc1.f1 + wc2.f1);
            }
        });

        // WAL Sink 输出 需要等 Checkpoint 完成再输出
        result.transform("StdOutWriteAheadSink", Types.TUPLE(Types.STRING, Types.LONG), new StdOutWALSink());

        env.execute("StdOutWriteAheadSinkExample");
    }

    // 自定义实现 GenericWriteAheadSink
    private static class StdOutWALSink extends GenericWriteAheadSink<Tuple2<String, Long>> {
        // 构造函数
        public StdOutWALSink() throws Exception {
            super(
                    // CheckpointCommitter
                    new FileCheckpointCommitter(System.getProperty("java.io.tmpdir")),
                    // 用于序列化输入记录的 TypeSerializer
                    Types.<Tuple2<String, Long>>TUPLE(Types.STRING, Types.LONG).createSerializer(new ExecutionConfig()),
                    // 自定义作业 ID
                    UUID.randomUUID().toString()
            );
        }

        @Override
        protected boolean sendValues(Iterable<Tuple2<String, Long>> words, long checkpointId, long timestamp) throws Exception {
            // 输出到外部系统 在这为 StdOut 标准输出
            // 每次 Checkpoint 完成之后通过 notifyCheckpointComplete 调用该方法
            int subtask = getRuntimeContext().getIndexOfThisSubtask();
            for (Tuple2<String, Long> word : words) {
                System.out.println("StdOut> " + word);
                LOG.info("checkpointId {} (subTask = {}) send word: {}", checkpointId, subtask, word);
            }
            return true;
        }
    }

}
