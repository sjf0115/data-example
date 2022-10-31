package com.common.example.bean;

/**
 * 功能：登录用户
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2022/10/20 下午8:55
 */
public class LoginUser {
    private Integer appId;
    private Long uid;
    private String os;
    private Long timestamp;

    public LoginUser() {
    }

    public LoginUser(Integer appId, Long uid) {
        this.appId = appId;
        this.uid = uid;
    }

    public LoginUser(Integer appId, Long uid, String os, Long timestamp) {
        this.appId = appId;
        this.uid = uid;
        this.os = os;
        this.timestamp = timestamp;
    }

    public Integer getAppId() {
        return appId;
    }

    public void setAppId(Integer appId) {
        this.appId = appId;
    }

    public Long getUid() {
        return uid;
    }

    public void setUid(Long uid) {
        this.uid = uid;
    }

    public String getOs() {
        return os;
    }

    public void setOs(String os) {
        this.os = os;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }
}
