package com.flink.example.monitor;

import com.codahale.metrics.SlidingWindowReservoir;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 功能：Histogram Metric 示例
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/7/16 下午4:17
 */
public class HistogramMetricExample {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> text = env.socketTextStream("localhost", 9000, "\n");
        DataStream<Long> wordsStream = text.map(new WordsMapFunction());
        wordsStream.print();
        env.execute("histogram-metric-example");
    }

    public static class WordsMapFunction extends RichMapFunction<String, Long> {
        private transient Histogram histogram;
        @Override
        public void open(Configuration parameters) throws Exception {
            // 创建 Codahale/DropWizard Histograms
            com.codahale.metrics.Histogram dropwizardHistogram =
                    new com.codahale.metrics.Histogram(new SlidingWindowReservoir(500));
            // 使用 DropwizardHistogramWrapper 包装类转换
            this.histogram = getRuntimeContext()
                    .getMetricGroup()
                    .histogram("words-histogram", new DropwizardHistogramWrapper(dropwizardHistogram));
        }
        @Override
        public Long map(String value) throws Exception {
            Long v = Long.parseLong(value);
            // 更新直方图
            histogram.update(v);
            return v;
        }
    }
}
