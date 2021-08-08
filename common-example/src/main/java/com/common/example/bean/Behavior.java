package com.common.example.bean;

/**
 * 微博用户行为
 * Created by wy on 2020/10/27.
 */
public class Behavior {
    // 用户Id
    private String uid;
    // 微博Id
    private String wid;
    // 发微博时间
    private String tm;
    // 微博内容
    private String content;

    public Behavior(String uid, String wid, String tm, String content) {
        this.uid = uid;
        this.wid = wid;
        this.tm = tm;
        this.content = content;
    }

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public String getWid() {
        return wid;
    }

    public void setWid(String wid) {
        this.wid = wid;
    }

    public String getTm() {
        return tm;
    }

    public void setTm(String tm) {
        this.tm = tm;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }
}
