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
        // 创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tEnv = BatchTableEnvironment.create(env);

        // 读取数据创建 DataSet
        DataSet<WordCount> input = env.fromElements(
                new WordCount("Hello", 1L),
                new WordCount("Ciao", 1L),
                new WordCount("Hello", 1L));

        // DataSet 转换为 Table
        Table table = tEnv.fromDataSet(input);

        // 执行查询
        Table resultTable = table
                .groupBy("word")
                .select("word, frequency.sum as frequency");

        // Table 转换为 DataSet
        DataSet<WordCount> result = tEnv.toDataSet(resultTable, WordCount.class);

        // 输出
        result.print();
    }
}
