package com.common.example.jmx.bean;

import java.util.List;

/**
 * 自定义 ResourceMBean
 */
public interface ResourceMBean {
    public String getLastItem();
    public int getSize();
    public void addItem(String item);
    public List<String> getItems();
    public String getItem(int pos);
}
