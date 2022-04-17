package com.flink.example.stream.base.partitioner;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 自定义Partitioner 示例
 * Created by wy on 2021/3/21.
 */
public class CustomPartitionerExample {
    private static final Logger LOG = LoggerFactory.getLogger(CustomPartitionerExample.class);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // CustomPartitionerWrapper
        DataStream<String> result = env.socketTextStream("localhost", 9100, "\n")
                .map(str -> str.toLowerCase()).name("LowerCaseMap").setParallelism(3)
                .partitionCustom(new MyCustomPartitioner(), new KeySelector<String, String>() {
                    @Override
                    public String getKey(String value) throws Exception {
                        return value;
                    }
                })
                .map(str -> str.toUpperCase()).name("UpperCaseMap").setParallelism(3);

        result.print();

        env.execute("CustomPartitionerExample");
    }

    /**
     * 自定义Partitioner
     */
    private static class MyCustomPartitioner implements Partitioner<String> {
        @Override
        public int partition(String key, int numPartitions) {
            // 全部大写分发到 0 实例
            if (StringUtils.isAllUpperCase(key)) {
                return 0;
            }
            // 其他分发到 1 实例
            return 1;
        }
    }
}
