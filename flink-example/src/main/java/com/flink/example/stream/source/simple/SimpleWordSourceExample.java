package com.flink.example.stream.source.simple;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 功能：SimpleWordSource 示例
 * 作者：SmartSi
 * CSDN博客：https://blog.csdn.net/sunnyyoona
 * 公众号：大数据生态
 * 日期：2022/8/21 下午2:43
 */
public class SimpleWordSourceExample {
    private static final Logger LOG = LoggerFactory.getLogger(SimpleWordSourceExample.class);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 随机生成单词
        DataStream<String> word = env.addSource(new SimpleWordSource(10000L));
        // 输出单词
        word.print();
        env.execute("SimpleWordSourceExample");
    }
}
