package com.flink.example.stream.function.split;

import com.flink.example.stream.function.SideOutputExample;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 功能：Filter 过滤示例
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2022/9/7 上午9:00
 */
public class FilterExample {
    private static final Logger LOG = LoggerFactory.getLogger(SideOutputExample.class);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //输入数据源
        DataStreamSource<Integer> source = env.fromElements(1, 2, 3, 4, 5, 6, 7, 8);

        // 奇数流
        SingleOutputStreamOperator<Integer> oddStream = source.filter(new FilterFunction<Integer>() {
            @Override
            public boolean filter(Integer num) throws Exception {
                return num % 2 != 0;
            }
        });

        // 偶数流
        SingleOutputStreamOperator<Integer> evenStream = source.filter(new FilterFunction<Integer>() {
            @Override
            public boolean filter(Integer num) throws Exception {
                return num % 2 == 0;
            }
        });

        // 输出
        oddStream.print("ODD");
        evenStream.print("EVEN");

        env.execute("FilterExample");
    }
}
