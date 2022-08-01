package com.flink.example.monitor;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 功能：Counter Metric 示例
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/7/16 下午4:02
 */
public class CounterMetricExample {

    private static final Logger LOG = LoggerFactory.getLogger(CounterMetricExample.class);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> text = env.socketTextStream("localhost", 9000, "\n").setParallelism(1);
        DataStream<String> wordsStream = text.flatMap(new WordsFlatMapFunction()).setParallelism(2);
        wordsStream.print().setParallelism(1);
        env.execute("counter-metric-example");
    }

    public static class WordsFlatMapFunction extends RichFlatMapFunction<String, String> {
        private transient Counter counter;
        @Override
        public void open(Configuration parameters) throws Exception {
            // 注册 Counter
            this.counter = getRuntimeContext()
                    .getMetricGroup()
                    .addGroup("UserCustomMetric")
                    .counter("WordsCounter");
        }

        @Override
        public void flatMap(String value, Collector<String> collector) throws Exception {
            for (String word : value.split("\\s")) {
                // 计数
                counter.inc();
                LOG.info("{}: {}", word, counter.getCount());
                collector.collect(word);
            }
        }
    }
}
