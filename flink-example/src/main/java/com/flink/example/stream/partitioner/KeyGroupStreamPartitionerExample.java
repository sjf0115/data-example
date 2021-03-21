package com.flink.example.stream.partitioner;

import com.flink.example.stream.state.state.KeyGroupExample;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * KeyGroupStreamPartitioner 示例
 * Created by wy on 2021/3/14.
 */
public class KeyGroupStreamPartitionerExample {
    private static final Logger LOG = LoggerFactory.getLogger(KeyGroupExample.class);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // KeyGroupStreamPartitioner
        DataStream<String> result = env.socketTextStream("localhost", 9100, "\n")
                .map(str -> str.toLowerCase()).name("LowerCaseMap").setParallelism(2)
                .keyBy(str -> str)
                .map(str -> str.toUpperCase()).name("UpperCaseMap").setParallelism(2);

        result.print();

        env.execute("KeyGroupStreamPartitionerExample");
    }
}
