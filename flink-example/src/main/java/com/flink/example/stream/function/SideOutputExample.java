package com.flink.example.stream.function;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 功能：旁路输出
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2021/6/20 下午12:55
 */
public class SideOutputExample {
    private static final Logger LOG = LoggerFactory.getLogger(SideOutputExample.class);
    // 奇数
    private static final OutputTag<Integer> oddOutputTag = new OutputTag<Integer>("odd-side-output"){};
    // 偶数
    private static final OutputTag<Integer> evenOutputTag = new OutputTag<Integer>("even-side-output"){};

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置事件时间特性
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<String> source = env.socketTextStream("localhost", 9100, "\n");
        SingleOutputStreamOperator<String> mainStream = source.process(new MyProcessFunction());
        mainStream.print("MainStream");

        // 旁路输出 奇数
        DataStream<Integer> oddSideOutputStream = mainStream.getSideOutput(oddOutputTag);
        oddSideOutputStream.print("OddSideOutputStream");

        // 旁路输出 偶数
        DataStream<Integer> evenSideOutputStream = mainStream.getSideOutput(evenOutputTag);
        evenSideOutputStream.print("EvenSideOutputStream");

        env.execute("SideOutputExample");
    }

    /**
     * 自定义ProcessFunction
     */
    private static class MyProcessFunction extends ProcessFunction<String, String> {

        @Override
        public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
            // 主输出
            out.collect(value);
            // 旁路输出 可以输出不同的数据类型
            Integer val = Integer.parseInt(value);
            if (val % 2 != 0) {
                // 奇数输出
                ctx.output(oddOutputTag, val);
            } else {
                // 偶数输出
                ctx.output(evenOutputTag, val);
            }
        }
    }
}
// 输入数据示例
// 1
// 2
// 3
// 4
// 5
// 6