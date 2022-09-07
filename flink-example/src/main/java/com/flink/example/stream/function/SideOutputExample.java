package com.flink.example.stream.function;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
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

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // OutputTag 这里类型不一致 主要是为了验证不同侧输出流可以输出不同的类型
        OutputTag<String> oddOutputTag = new OutputTag<String>("ODD"){};
        OutputTag<Long> evenOutputTag = new OutputTag<Long>("EVEN"){};

        //输入数据源
        DataStreamSource<Integer> source = env.fromElements(1, 2, 3, 4, 5, 6, 7, 8);
        // 拆分偶数流和奇数流
        SingleOutputStreamOperator<Integer> mainStream = source.process(new ProcessFunction<Integer, Integer>() {
            @Override
            public void processElement(Integer num, Context ctx, Collector<Integer> out) throws Exception {
                // 主输出
                out.collect(num);
                // 旁路输出 可以输出不同的数据类型
                if (num % 2 != 0) {
                    // 奇数输出
                    ctx.output(oddOutputTag, "O-" + num);
                } else {
                    // 偶数输出
                    ctx.output(evenOutputTag, Long.valueOf(num));
                }
            }
        });

        // 主链路
        mainStream.print("ALL");
        // 旁路输出 奇数
        DataStream<String> oddStream = mainStream.getSideOutput(oddOutputTag);
        oddStream.print("ODD");
        // 旁路输出 偶数
        DataStream<Long> evenStream = mainStream.getSideOutput(evenOutputTag);
        evenStream.print("EVEN");

        env.execute("SideOutputExample");
    }
}
