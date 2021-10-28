package com.flink.example.stream.partitioner;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ForwardPartitioner 示例
 * Created by wy on 2021/3/14.
 */
public class ForwardPartitionerExample {
    private static final Logger LOG = LoggerFactory.getLogger(ForwardPartitionerExample.class);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // ForwardPartitioner
        DataStream<String> result = env.socketTextStream("localhost", 9100, "\n")
                .map(str -> str.toLowerCase()).name("LowerCaseMap").setParallelism(3)
                .forward()
                .map(str -> str.toUpperCase()).name("UpperCaseMap").setParallelism(3).disableChaining();

        result.print();

        env.execute("ForwardPartitionerExample");
    }
}
