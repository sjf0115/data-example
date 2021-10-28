package com.flink.example.stream.partitioner;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * RebalancePartitioner 示例
 * Created by wy on 2021/3/14.
 */
public class RebalancePartitionerExample {
    private static final Logger LOG = LoggerFactory.getLogger(RebalancePartitionerExample.class);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // RebalancePartitioner
        DataStream<String> result = env.socketTextStream("localhost", 9100, "\n")
                .map(str -> str.toLowerCase()).name("LowerCaseMap").setParallelism(1)
                .rebalance()
                .map(str -> str.toUpperCase()).name("UpperCaseMap").setParallelism(2);

        result.print();

        env.execute("RebalancePartitionerExample");
    }
}
