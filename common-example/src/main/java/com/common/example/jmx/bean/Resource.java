package com.common.example.jmx.bean;

import java.util.ArrayList;
import java.util.List;

/**
 * 功能：实现自定义 ResourceMBean
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/6/2 下午4:13
 */
public class Resource implements ResourceMBean {
    private List<String> items = new ArrayList<>();

    @Override
    public String getLastItem() {
        return items.get(getSize()-1);
    }

    @Override
    public int getSize() {
        return items.size();
    }

    @Override
    public List<String> getItems() {
        return items;
    }

    @Override
    public void addItem(String item) {
        items.add(item);
    }

    @Override
    public String getItem(int pos) {
        return items.get(pos);
    }
}
