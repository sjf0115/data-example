package com.hive.example.udf;

import com.common.example.utils.BitmapFunction;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.parquet.io.api.Binary;

/**
 * 功能：Bitmap 基数
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2022/11/6 下午6:09
 */
public class BitmapCountUDF extends UDF {
    public Long evaluate(Binary b1) {
        Long result = BitmapFunction.bitmapCount(b1);
        return result;
    }
    public Long evaluate(String str) {
        Long result = BitmapFunction.bitmapCount(str);
        return result;
    }
}
