package com.common.example.generics;

/**
 * 功能：StrPair
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2021/10/6 下午8:11
 */
public class StrPair extends Pair<String> {
    @Override
    public String getValue() {
        return super.getValue();
    }

    @Override
    public void setValue(String value) {
        super.setValue(value);
    }
}
