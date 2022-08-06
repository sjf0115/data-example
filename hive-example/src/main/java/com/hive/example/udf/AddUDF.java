package com.hive.example.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;

/**
 * 功能：Add UDF
 * 作者：SmartSi
 * CSDN博客：https://blog.csdn.net/sunnyyoona
 * 公众号：大数据生态
 * 日期：2022/8/6 下午10:33
 */
@Description(name = "custom_add", value = "_FUNC_(expr) - Example UDAF that returns the sum")
public class AddUDF extends UDF {
    // Int 加和
    public IntWritable evaluate(IntWritable num1, IntWritable num2){
        if(num1 == null || num2 == null){
            return null;
        }
        return new IntWritable(num1.get() + num2.get());
    }
    // Double 加和
    public DoubleWritable evaluate(DoubleWritable num1, DoubleWritable num2){
        if(num1 == null || num2 == null){
            return null;
        }
        return new DoubleWritable(num1.get() + num2.get());
    }
}
