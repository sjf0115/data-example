package com.flink.example.stream.window.function;

import com.common.example.utils.DateUtil;
import com.flink.example.stream.source.simple.SimpleWordSource;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 功能：ReduceFunction 增量函数与 ProcessWindowFunction 组合使用
 *          实现单词求和
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2021/2/15 下午4:20
 */
public class ReduceProcessWindowFunctionExample {
    private static final Logger LOG = LoggerFactory.getLogger(ReduceProcessWindowFunctionExample.class);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000L);

        // Stream of (word) 每10s输出一个单词 最多输出20次
        DataStream<String> source = env.addSource(new SimpleWordSource(10*1000L, 20));

        // Stream of (word, 1)
        DataStream<Tuple2<String, Integer>> wordsCount = source.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector out) {
                for (String word : value.split("\\s")) {
                    out.collect(Tuple2.of(word, 1));
                }
            }
        });

        // 滚动窗口
        SingleOutputStreamOperator<Tuple4<String, Integer, String, String>> result = wordsCount
                // 根据单词分组
                .keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Integer> value) throws Exception {
                        LOG.info("[Source] word: {}", value.f0);
                        return value.f0;
                    }
                })
                // 窗口大小为1分钟的滚动窗口
                .window(TumblingProcessingTimeWindows.of(Time.minutes(1)))
                // ReduceFunction 和 ProcessWindowFunction 组合使用
                .reduce(new CountReduceFunction(), new WordsCountProcessWindowFunction());

        result.print();
        env.execute("ReduceProcessWindowFunctionExample");
    }

    /**
     * 自定义ReduceFunction：
     *      计算每个单词出现的个数
     */
    private static class CountReduceFunction implements ReduceFunction<Tuple2<String, Integer>> {
        public Tuple2<String, Integer> reduce(Tuple2<String, Integer> wordCount1, Tuple2<String, Integer> wordCount2) {
            int count = wordCount1.f1 + wordCount2.f1;
            LOG.info("[ReduceFunction] word: {}, count: {}", wordCount1.f0, count);
            return new Tuple2(wordCount1.f0, count);
        }
    }

    /**
     * 自定义ProcessWindowFunction：
     *      获取窗口元信息
     */
    private static class WordsCountProcessWindowFunction extends ProcessWindowFunction<
            Tuple2<String, Integer>,
            Tuple4<String, Integer, String, String>,
            String,
            TimeWindow> {
        @Override
        public void process(String s, Context context, Iterable<Tuple2<String, Integer>> elements, Collector<Tuple4<String, Integer, String, String>> out) throws Exception {
            // 窗口聚合结果值
            Tuple2<String, Integer> wordCount = elements.iterator().next();
            String word = wordCount.f0;
            Integer count = wordCount.f1;
            // 窗口元信息
            TimeWindow window = context.window();
            long start = window.getStart();
            long end = window.getEnd();
            String startTime = DateUtil.timeStamp2Date(start, "yyyy-MM-dd HH:mm:ss");
            String endTime = DateUtil.timeStamp2Date(end, "yyyy-MM-dd HH:mm:ss");
            // 当前处理时间
            long currentProcessingTimeStamp = context.currentProcessingTime();
            String currentProcessingTime = DateUtil.timeStamp2Date(currentProcessingTimeStamp, "yyyy-MM-dd HH:mm:ss");
            LOG.info("[ProcessWindowFunction] word: {}, count: {}, window: {}, processingTime: {}",
                    word, count,
                    "[" + startTime + ", " + endTime + "]", currentProcessingTime
            );
            // 输出
            out.collect(Tuple4.of(word, count, startTime, endTime));
        }
    }
}
