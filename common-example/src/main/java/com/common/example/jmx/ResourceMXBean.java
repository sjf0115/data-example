package com.common.example.jmx;

import com.common.example.bean.ResourceItem;

import java.util.List;

/**
 * 功能：ResourceMXBean 接口
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/6/2 下午8:57
 */
public interface ResourceMXBean {
    public ResourceItem getLastItem();
    public int getSize();
    public List<ResourceItem> getItems();

    public void addItem(ResourceItem item);
    public ResourceItem getItem(int pos);
}
