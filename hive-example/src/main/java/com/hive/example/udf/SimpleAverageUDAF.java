package com.hive.example.udf;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluatorResolver;

/**
 * 功能：Simple 实现方式
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2021/12/4 上午11:00
 */
public class SimpleAverageUDAF extends UDAF {

    public SimpleAverageUDAF(UDAFEvaluatorResolver rslv) {
        super(rslv);
    }

    @Override
    public void setResolver(UDAFEvaluatorResolver rslv) {
        super.setResolver(rslv);
    }

    @Override
    public UDAFEvaluatorResolver getResolver() {
        return super.getResolver();
    }


}
