package com.flink.example.base;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
import scala.Int;

import java.util.List;

/**
 * Created by wy on 2020/10/31.
 */
public class BroadcastSetExample {

    public static void main(String[] args) {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        String broadcastSetName = "BroadcastSetName";

        // 1. 待广播的数据集
        DataSet<String> broadcastSet = env.fromElements("a", "c", "f");
        DataSet<String> data = env.fromElements("a", "b", "c", "d", "e", "f");
        data.map(new RichMapFunction<String, String>() {
            List<Int> broadcastVariable;
            @Override
            public void open(Configuration parameters) throws Exception {
                // 3. 获取广播变量
                broadcastVariable = getRuntimeContext().getBroadcastVariable(broadcastSetName);
            }
            @Override
            public String map(String value) throws Exception {
                if (broadcastVariable.contains(value)) {
                    value = value.toUpperCase();
                }
                System.out.println(value);
                return value;
            }
        }).withBroadcastSet(broadcastSet, broadcastSetName);// 2. 广播数据集
    }
}
