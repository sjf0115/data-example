package com.flink.example.stream.function;

import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * 功能：Split 分流示例
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2022/9/6 下午10:51
 */
public class SplitExample {
    private static final Logger LOG = LoggerFactory.getLogger(SplitExample.class);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
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
                    LOG.info("偶数: {}", num);
                } else {
                    // 奇数打标 ODD
                    tags.add("ODD");
                    LOG.info("奇数: {}", num);
                }
                return tags;
            }
        });

        splitStream.select("EVEN").print("偶数");
        splitStream.select("ODD").print("奇数");
        env.execute("SplitExample");
    }
}
