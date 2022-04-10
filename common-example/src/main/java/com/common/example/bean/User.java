package com.common.example.bean;

import java.time.Instant;

/**
 * 功能：POJO User
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/4/10 下午1:47
 */
public class User {
    public String name;
    public Integer score;
    public Instant eventTime;

    public User() {
    }

    public User(String name, Integer score, Instant eventTime) {
        this.name = name;
        this.score = score;
        this.eventTime = eventTime;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getScore() {
        return score;
    }

    public void setScore(Integer score) {
        this.score = score;
    }

    public Instant getEventTime() {
        return eventTime;
    }

    public void setEventTime(Instant eventTime) {
        this.eventTime = eventTime;
    }

    @Override
    public String toString() {
        return "name: " + name + ", score: " + score + ", eventTime: " + eventTime;
    }
}
