package com.flink.example.stream.sink.wal;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * 功能：StdOutWriteAheadSink 示例
 * 作者：SmartSi
 * CSDN博客：https://blog.csdn.net/sunnyyoona
 * 公众号：大数据生态
 * 日期：2022/8/20 下午3:29
 */
public class StdOutWriteAheadSinkExample {

    private static final Logger LOG = LoggerFactory.getLogger(StdOutWriteAheadSinkExample.class);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 每隔1分钟进行一次 Checkpoint 如果不设置 Checkpoint 自定义 WAL Sink 不会输出数据
        env.enableCheckpointing(60 * 1000);
        // 重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                3, // 重启最大次数
                Time.of(10, TimeUnit.SECONDS) // 重启时间间隔
        ));

        // Socket 输入
        DataStream<String> stream = env.socketTextStream("localhost", 9100, "\n");

        // 单词流
        DataStream<Tuple2<String, Integer>> words = stream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector out) {
                for (String word : value.split("\\s")) {
                    out.collect(Tuple2.of(word, 1));
                }
            }
        });

        // 单词个数流
        DataStream<Tuple2<String, Integer>> wordsCount = words.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> tuple2) throws Exception {
                // 模拟程序 Failover 遇到 error 抛出异常
                if (Objects.equals(tuple2.f0, "error")) {
                    throw new RuntimeException("模拟程序 Failover");
                }
                return tuple2.f0;
            }
        }).reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2 reduce(Tuple2<String, Integer> a, Tuple2<String, Integer> b) {
                int sum = a.f1 + b.f1;
                LOG.info("[Reduce] {}:{}", a.f0, sum);
                return new Tuple2(a.f0, sum);
            }
        });

        // WAL Sink 输出 需要等 Checkpoint 完成再输出
        wordsCount.transform(
                "StdOutWriteAheadSink",
                Types.TUPLE(Types.STRING, Types.INT),
                new StdOutWriteAheadSink()
        );
        // Print 输出 立马输出
        wordsCount.print("Print");

        env.execute("StdOutWriteAheadSinkExample");
    }
}
