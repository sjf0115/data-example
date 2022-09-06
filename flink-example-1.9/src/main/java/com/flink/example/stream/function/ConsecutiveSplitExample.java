package com.flink.example.stream.function;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * 功能：连续拆分流
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2022/9/6 下午11:48
 */
public class ConsecutiveSplitExample {
    private static final Logger LOG = LoggerFactory.getLogger(SplitExample.class);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //输入数据源
        DataStreamSource<Integer> source = env.fromElements(1, 2, 3, 4, 5, 6, 7, 8);

        // 分流
        SplitStream<Integer> splitStream = source.split(new OutputSelector<Integer>() {
            @Override
            public Iterable<String> select(Integer num) {
                // 给数据打标
                List<String> tags = new ArrayList<>();
                if (num % 2 == 0) {
                    // 偶数打标 EVEN
                    tags.add("EVEN");
                } else {
                    // 奇数打标 ODD
                    tags.add("ODD");
                }
                return tags;
            }
        });

        // 偶数流
        DataStream<Integer> evenStream = splitStream.select("EVEN");
        // 输出
        evenStream.print("偶数");

        // 拆分奇数流 小于5的一个流 大于等于5的一个流
        DataStream<Integer> oddStream = splitStream.select("ODD");
        SplitStream<Integer> oddSplitStream = oddStream
                .map(new MapFunction<Integer, Integer>() {
                    @Override
                    public Integer map(Integer num) throws Exception {
                        return num;
                    }
                })
                .split(new OutputSelector<Integer>() {
                    @Override
                    public Iterable<String> select(Integer num) {
                        // 给数据打标
                        List<String> tags = new ArrayList<>();
                        if (num < 5) {
                            // 小于5
                            tags.add("LESS");
                        } else {
                            //大于等于5
                            tags.add("MORE");
                        }
                        return tags;
                    }
                });
        // 输出
        oddSplitStream.select("LESS").print("小于");
        oddSplitStream.select("MORE").print("大于等于");

        env.execute("ConsecutiveSplitExample");
    }
}
