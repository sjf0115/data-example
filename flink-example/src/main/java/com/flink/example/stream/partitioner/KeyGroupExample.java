package com.flink.example.stream.partitioner;

import com.google.common.collect.Lists;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Created by wy on 2021/3/7.
 */
public class KeyGroupExample {

    private static final Logger LOG = LoggerFactory.getLogger(KeyGroupExample.class);

    public static void main(String[] args) throws Exception {
        // 算子并发：3
        int parallelism = 3;

        // 最大并：128 对应有 128 个 KeyGroup
        int maxParallelism = KeyGroupRangeAssignment.computeDefaultMaxParallelism(parallelism);
        System.out.println(maxParallelism);

        for (int i = 0; i < parallelism;i++) {
            KeyGroupRange keyGroupRange = KeyGroupRangeAssignment.computeKeyGroupRangeForOperatorIndex(maxParallelism, parallelism, i);
            System.out.println("SubTask" + i + " -> KeyGroupRange [" + keyGroupRange.getStartKeyGroup() + ", " + keyGroupRange.getEndKeyGroup() + "]");
        }

        // 通过KeyGroupId映射SubTaskId(OperatorIndex)
        int subTaskId = KeyGroupRangeAssignment.computeOperatorIndexForKeyGroup(maxParallelism, parallelism, 2);
        System.out.println("KeyGroupId: 2 -> SubTaskId: " + subTaskId);

        // 通过SubTaskId(OperatorIndex)映射KeyGroupId
        KeyGroupRange keyGroupRange = KeyGroupRangeAssignment.computeKeyGroupRangeForOperatorIndex(maxParallelism, parallelism, 1);
        System.out.println("SubTaskId: 1 -> keyGroupRange: [" + keyGroupRange.getStartKeyGroup() + ", " + keyGroupRange.getEndKeyGroup() + "]");

        // Key: A 在 232 KeyGroup 上

        List<String> list = Lists.newArrayList("A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L");
        for(String key : list) {
            int keyGroupId = KeyGroupRangeAssignment.computeKeyGroupForKeyHash(key.hashCode(), maxParallelism);
            subTaskId = KeyGroupRangeAssignment.computeOperatorIndexForKeyGroup(maxParallelism, parallelism, keyGroupId);
            System.out.println("Key: " + key + ", KeyGroupId: " + keyGroupId + ", SubTaskId: " + subTaskId);
        }
    }
}
