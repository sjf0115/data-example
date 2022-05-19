package com.flink.example.table.function.custom;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.util.Objects;

/**
 * 功能：自定义表函数 Split
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/5/18 下午11:21
 */
@FunctionHint(output = @DataTypeHint("Row<word STRING>"))
public class SplitTableFunction extends TableFunction<Row> {

    // 分隔符
    private String sep = ",";

    public SplitTableFunction() {
    }

    public SplitTableFunction(String sep) {
        this.sep = sep;
    }

    // 单字符串
    public void eval(String str) {
        if (Objects.equals(str, null)) {
            return;
        }
        String[] splits = str.split(sep);
        for (String s : splits) {
            collect(Row.of(s));
        }
    }

    // 变长参数
    public void eval(String ... values) {
        for (String s : values) {
            collect(Row.of(s));
        }
    }
}
