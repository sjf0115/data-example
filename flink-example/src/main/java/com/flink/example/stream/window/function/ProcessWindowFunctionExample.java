package com.flink.example.stream.window.function;

import com.common.example.utils.DateUtil;
import com.flink.example.stream.sink.print.PrintLogSinkFunction;
import com.flink.example.stream.source.simple.SimpleTemperatureSource;
import com.google.common.collect.Lists;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * 功能：窗口 ProcessWindowFunction 示例
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2021/2/14 下午4:20
 */
public class ProcessWindowFunctionExample {

    private static final Logger LOG = LoggerFactory.getLogger(ProcessWindowFunctionExample.class);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000L);

        // Stream of (id, temperature) 每10s输出一个传感器温度 最多输出20次
        DataStreamSource<Tuple2<String, Integer>> source = env.addSource(new SimpleTemperatureSource(10*1000L, 20));

        // 最低温度和最高温度
        SingleOutputStreamOperator<Tuple3<String, Integer, Integer>> stream = source
                // 分组
                .keyBy(new KeySelector<Tuple2<String, Integer>, String>() {

                    @Override
                    public String getKey(Tuple2<String, Integer> value) throws Exception {
                        return value.f0;
                    }
                })
                // 窗口大小为1分钟的滚动窗口
                .window(TumblingProcessingTimeWindows.of(Time.minutes(1)))
                // 窗口函数
                .process(new HighLowTemperatureProcessWindowFunction());

        // stream.print();
        // 代替 print() 方法 输出到控制台并打印日志
        stream.addSink(new PrintLogSinkFunction());
        env.execute("ProcessWindowFunctionExample");
    }

    /**
     * 自定义实现 ProcessWindowFunction
     *      计算最高温度和最低温度
     */
    private static class HighLowTemperatureProcessWindowFunction extends ProcessWindowFunction<
            Tuple2<String, Integer>, // 输入类型
            Tuple3<String, Integer, Integer>, // 输出类型
            String, // key 类型
            TimeWindow> {
        @Override
        public void process(String key, Context context, Iterable<Tuple2<String, Integer>> elements, Collector<Tuple3<String, Integer, Integer>> out) throws Exception {
            int lowTemperature = Integer.MAX_VALUE;
            int highTemperature = Integer.MIN_VALUE;
            List<Integer> temperatures = Lists.newArrayList();
            for (Tuple2<String, Integer> element : elements) {
                // 温度列表
                temperatures.add(element.f1);
                Integer temperature = element.f1;
                // 计算最低温度
                if (temperature < lowTemperature) {
                    lowTemperature = temperature;
                }
                // 计算最高温度
                if (temperature > highTemperature) {
                    highTemperature = temperature;
                }
            }
            // 时间窗口元数据
            TimeWindow window = context.window();
            long start = window.getStart();
            long end = window.getEnd();
            String startTime = DateUtil.timeStamp2Date(start, "yyyy-MM-dd HH:mm:ss");
            String endTime = DateUtil.timeStamp2Date(end, "yyyy-MM-dd HH:mm:ss");
            // 当前处理时间
            long currentProcessingTimeStamp = context.currentProcessingTime();
            String currentProcessingTime = DateUtil.timeStamp2Date(currentProcessingTimeStamp, "yyyy-MM-dd HH:mm:ss");
            LOG.info("sensorId: {}, List: {}, Low: {}, High: {}, Window: {}, ProcessingTime: {}",
                    key, temperatures, lowTemperature, highTemperature,
                    "[" + startTime + ", " + endTime + "]", currentProcessingTime
            );
            out.collect(Tuple3.of(temperatures.toString(), lowTemperature, highTemperature));
        }
    }
}
