package com.common.example.jmx.bean;

/**
 * 功能：用户黑名单管理接口
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/6/2 下午11:00
 */
public interface BlackListMBean {
    // 获取黑名单列表
    public String[] getBlackList();
    // 获取黑名单大小
    public int getBlackListSize();

    // 在黑名单列表中添加用户
    public void addBlackItem(String uid);
    // 从黑名单中移除用户
    public void removeBlackItem(String uid);
    // 判断某个用户是否在黑名单中
    public boolean contains(String uid);
}
