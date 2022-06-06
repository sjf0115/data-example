package com.common.example.jmx.bean;

import java.util.HashSet;
import java.util.Set;

/**
 * 功能：用户黑名单管理实现
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/6/2 下午11:05
 */
public class BlackList implements BlackListMBean {
    private Set<String> uidSet = new HashSet<>();

    @Override
    public String[] getBlackList() {
        return uidSet.toArray(new String[0]);
    }

    @Override
    public void addBlackItem(String uid) {
        uidSet.add(uid);
    }

    @Override
    public void removeBlackItem(String uid) {
        uidSet.remove(uid);
    }

    @Override
    public boolean contains(String uid) {
        return uidSet.contains(uid);
    }

    @Override
    public int getBlackListSize() {
        return uidSet.size();
    }
}
