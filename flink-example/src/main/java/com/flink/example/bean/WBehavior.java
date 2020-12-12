package com.flink.example.bean;

/**
 * 微博用户行为
 * Created by wy on 2020/10/27.
 */
public class WBehavior {
    private String uid;
    private String wid;
    private String time;
    private String content;

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

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }
}
