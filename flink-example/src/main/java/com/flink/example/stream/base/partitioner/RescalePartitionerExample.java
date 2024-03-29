package com.flink.example.stream.base.partitioner;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * RescalePartitioner 示例
 * Created by wy on 2021/3/14.
 */
public class RescalePartitionerExample {
    private static final Logger LOG = LoggerFactory.getLogger(KeyGroupExample.class);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // RescalePartitioner
        DataStream<String> result = env.socketTextStream("localhost", 9100, "\n")
                .map(str -> str.toLowerCase()).name("LowerCaseMap").setParallelism(1)
                .rescale()
                .map(str -> str.toUpperCase()).name("UpperCaseMap").setParallelism(2);

        result.print();

        env.execute("RescalePartitionerExample");
    }
}
