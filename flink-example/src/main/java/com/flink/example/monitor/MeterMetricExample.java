package com.flink.example.monitor;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper;
import org.apache.flink.metrics.Meter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 功能：Meter Metric 示例
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/7/17 下午10:15
 */
public class MeterMetricExample {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> text = env.socketTextStream("localhost", 9000, "\n");
        DataStream<String> wordsStream = text.flatMap(new WordsFlatMapFunction());
        wordsStream.print();
        env.execute("meter-metric-example");
    }

    public static class WordsFlatMapFunction extends RichFlatMapFunction<String, String> {
        private transient Meter meter;
        @Override
        public void open(Configuration parameters) throws Exception {
            // 创建 Codahale/DropWizard Meter
            com.codahale.metrics.Meter dropwizardMeter = new com.codahale.metrics.Meter();
            // 使用 DropwizardHistogramWrapper 包装类转换
            this.meter = getRuntimeContext()
                    .getMetricGroup()
                    .meter("words-meter", new DropwizardMeterWrapper(dropwizardMeter));
        }

        @Override
        public void flatMap(String value, Collector<String> collector) throws Exception {
            String[] words = value.split("\\s");
            for (String word : words) {
                // 标记一个单词出现
                this.meter.markEvent();
                collector.collect(word);
            }
        }
    }
}
