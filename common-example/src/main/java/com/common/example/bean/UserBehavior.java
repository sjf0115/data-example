package com.common.example.bean;
/**
 * 功能：淘宝用户行为
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/5/3 下午10:48
 */
public class UserBehavior {
    private Long uid;
    private Long pid;
    private Long cid;
    private String type;
    private Long ts;

    public UserBehavior() {
    }

    public UserBehavior(Long uid, Long pid, Long cid, String type, Long ts) {
        this.uid = uid;
        this.pid = pid;
        this.cid = cid;
        this.type = type;
        this.ts = ts;
    }

    public Long getUid() {
        return uid;
    }

    public void setUid(Long uid) {
        this.uid = uid;
    }

    public Long getPid() {
        return pid;
    }

    public void setPid(Long pid) {
        this.pid = pid;
    }

    public Long getCid() {
        return cid;
    }

    public void setCid(Long cid) {
        this.cid = cid;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Long getTs() {
        return ts;
    }

    public void setTs(Long ts) {
        this.ts = ts;
    }
}
