package com.flink.example.monitor;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 功能：Gauge Metric 示例
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/7/16 下午4:13
 */
public class GaugeMetricExample {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> text = env.socketTextStream("localhost", 9000, "\n");
        DataStream<String> wordsStream = text.flatMap(new WordsFlatMapFunction());
        wordsStream.print();
        env.execute("gauge-metric-example");
    }

    public static class WordsFlatMapFunction extends RichFlatMapFunction<String, String> {
        private transient int valueToExpose = 0;
        @Override
        public void open(Configuration parameters) throws Exception {
            // 注册 Gauge
            getRuntimeContext()
                    .getMetricGroup()
                    .gauge("words-gauge", new Gauge<Integer>() {
                        @Override
                        public Integer getValue() {
                            return valueToExpose;
                        }
                    });
        }

        @Override
        public void flatMap(String value, Collector<String> collector) throws Exception {
            for (String word : value.split("\\s")) {
                // 展示所有单词个数
                valueToExpose++;
                collector.collect(word);
            }
        }
    }
}
