package com.common.example.jmx;

import com.common.example.bean.ResourceItem;

import java.util.ArrayList;
import java.util.List;

/**
 * 功能：
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/6/2 下午9:47
 */
public class ResourceX implements ResourceMXBean {
    private List<ResourceItem> items = new ArrayList<>();
    @Override
    public ResourceItem getLastItem() {
        return items.get(getSize()-1);
    }

    @Override
    public int getSize() {
        return items.size();
    }

    @Override
    public List<ResourceItem> getItems() {
        return items;
    }

    @Override
    public void addItem(ResourceItem item) {
        items.add(item);
    }

    @Override
    public ResourceItem getItem(int pos) {
        return items.get(pos);
    }
}
