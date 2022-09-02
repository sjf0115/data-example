package com.flink.example.stream.window.function;

import com.common.example.utils.DateUtil;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * AggregateFunction Example
 * 实现功能：分组求平均值
 * Created by wy on 2021/2/12.
 */
/**
 * 功能：窗口 AverageAggregateFunction 示例 计算平均温度
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2022/8/28 下午4:20
 */
public class AggregateFunctionExample {
    private static final Logger LOG = LoggerFactory.getLogger(AggregateFunctionExample.class);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000L);

        // 设置事件时间特性
        DataStream<String> source = env.socketTextStream("localhost", 9100, "\n");

        // Stream of (dt, temperature)
        DataStream<Tuple3<String, Long, Integer>> stream = source.map(new MapFunction<String, Tuple3<String, Long, Integer>>() {
            @Override
            public Tuple3<String, Long, Integer> map(String str) throws Exception {
                String[] params = str.split(",");
                String key = params[0];
                String time = params[1];
                Long timeStamp = DateUtil.date2TimeStamp(time, "yyyy-MM-dd HH:mm:ss");
                Integer count = Integer.parseInt(params[2]);
                LOG.info("[Element] Key: " + key + ", timeStamp: [" + timeStamp + "|" + time + "], Count: " + count);
                return new Tuple3(key, timeStamp, count);
            }
        });

        DataStream<Tuple2<String, Double>> result = stream
                // 提取时间戳与设置Watermark
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple3<String, Long, Integer>>(Time.minutes(10)) {
                    @Override
                    public long extractTimestamp(Tuple3<String, Long, Integer> element) {
                        return element.f1;
                    }
                })
                // 格式转换
                .map(new MapFunction<Tuple3<String,Long,Integer>, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(Tuple3<String, Long, Integer> value) throws Exception {
                        return new Tuple2<String, Integer>(value.f0, value.f2);
                    }
                })
                // 分组
                .keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Integer> value) throws Exception {
                        return value.f0;
                    }
                })
                // 窗口大小为10分钟、滑动步长为5分钟的滑动窗口
                .timeWindow(Time.minutes(10), Time.minutes(5))
                .aggregate(new AverageAggregateFunction());

        result.print();
        env.execute("AggregateFunctionExample");
    }

    /**
     * 自定义AggregateFunction
     */
    private static class AverageAggregateFunction implements AggregateFunction<Tuple2<String, Integer>, Tuple3<String, Long, Long>, Tuple2<String, Double>> {

        // IN：Tuple2<String, Long>
        // ACC：Tuple3<String, Long, Long> -> <Key, Sum, Count>
        // OUT：Tuple2<String, Double>

        @Override
        public Tuple3<String, Long, Long> createAccumulator() {
            return new Tuple3<String, Long, Long>("", 0L, 0L);
        }

        @Override
        public Tuple3<String, Long, Long> add(Tuple2<String, Integer> value, Tuple3<String, Long, Long> accumulator) {
            return new Tuple3<String, Long, Long>(value.f0, accumulator.f1 + value.f1, accumulator.f2 + 1L);
        }

        @Override
        public Tuple2<String, Double> getResult(Tuple3<String, Long, Long> accumulator) {
            return new Tuple2<String, Double>(accumulator.f0, ((double) accumulator.f1) / accumulator.f2);
        }

        @Override
        public Tuple3<String, Long, Long> merge(Tuple3<String, Long, Long> a, Tuple3<String, Long, Long> b) {
            return new Tuple3<String, Long, Long>(a.f0, a.f1 + b.f1, a.f2 + b.f2);
        }
    }
    // 输入样例
//    A,2021-02-14 12:07:01,9
//    B,2021-02-14 12:08:01,5
//    A,2021-02-14 12:14:01,3
//    C,2021-02-14 12:09:01,2
//    C,2021-02-14 12:15:01,5
//    A,2021-02-14 12:08:01,4
//    B,2021-02-14 12:13:01,6
//    B,2021-02-14 12:21:01,1
//    D,2021-02-14 12:04:01,3
//    B,2021-02-14 12:26:01,2
//    B,2021-02-14 12:17:01,7
//    D,2021-02-14 12:09:01,8
//    C,2021-02-14 12:30:01,1
}
