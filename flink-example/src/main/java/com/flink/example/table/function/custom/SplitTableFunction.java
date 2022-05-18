package com.flink.example.table.function.custom;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

/**
 * 功能：自定义表函数 Split
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/5/18 下午11:21
 */
@FunctionHint(output = @DataTypeHint("ROW<word STRING, length INT>"))
public class SplitTableFunction extends TableFunction {
    public void eval(String str) {
        String[] splits = str.split("\\s+");
        for (String s : splits) {
            collect(Row.of(s, s.length()));
        }
    }
}
