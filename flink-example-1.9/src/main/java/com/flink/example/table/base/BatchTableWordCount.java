package com.flink.example.table.base;

import com.common.example.bean.WordCount;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;

/**
 * 功能：Table 版本 WordCount 批处理
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/4/9 下午3:01
 */
public class BatchTableWordCount {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tEnv = BatchTableEnvironment.create(env);

        DataSet<WordCount> input = env.fromElements(
                new WordCount("Hello", 1L),
                new WordCount("Ciao", 1L),
                new WordCount("Hello", 1L));

        Table table = tEnv.fromDataSet(input);

        Table filtered = table
                .groupBy("word")
                .select("word, frequency.sum as frequency")
                .filter("frequency = 2");

        DataSet<WordCount> result = tEnv.toDataSet(filtered, WordCount.class);

        result.print();
    }
}
