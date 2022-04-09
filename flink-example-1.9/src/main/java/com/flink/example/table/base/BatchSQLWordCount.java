package com.flink.example.table.base;

import com.common.example.bean.WordCount;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;

/**
 * 功能：SQL 版本 WordCount 批处理
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/4/9 下午3:06
 */
public class BatchSQLWordCount {
    public static void main(String[] args) throws Exception {
        // 执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tEnv = BatchTableEnvironment.create(env);

        DataSet<WordCount> input = env.fromElements(
                new WordCount("Hello", 1),
                new WordCount("Ciao", 1),
                new WordCount("Hello", 1));

        // 注册 Table
        tEnv.registerDataSet("WordCount", input, "word, frequency");

        // 执行 SQL 查询
        Table table = tEnv.sqlQuery(
                "SELECT word, SUM(frequency) as frequency FROM WordCount GROUP BY word");

        // Table 转换为 DataSet
        DataSet<WordCount> result = tEnv.toDataSet(table, WordCount.class);

        result.print();
    }
}
