package com.common.example.bean;

/**
 * Context对象
 * Created by wy on 2021/2/16.
 */
public class ContextInfo {
    private String key;
    private Integer sum;
    private String result;
    private String windowStartTime;
    private String windowEndTime;
    private String currentWatermark;
    private String currentProcessingTime;

    public String getWindowStartTime() {
        return windowStartTime;
    }

    public void setWindowStartTime(String windowStartTime) {
        this.windowStartTime = windowStartTime;
    }

    public String getWindowEndTime() {
        return windowEndTime;
    }

    public void setWindowEndTime(String windowEndTime) {
        this.windowEndTime = windowEndTime;
    }

    public String getCurrentWatermark() {
        return currentWatermark;
    }

    public void setCurrentWatermark(String currentWatermark) {
        this.currentWatermark = currentWatermark;
    }

    public String getCurrentProcessingTime() {
        return currentProcessingTime;
    }

    public void setCurrentProcessingTime(String currentProcessingTime) {
        this.currentProcessingTime = currentProcessingTime;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public Integer getSum() {
        return sum;
    }

    public void setSum(Integer sum) {
        this.sum = sum;
    }

    public String getResult() {
        return result;
    }

    public void setResult(String result) {
        this.result = result;
    }

    @Override
    public String toString() {
        return "{" +
                "key='" + key + '\'' +
                ", sum=" + sum +
                ", result='" + result + '\'' +
                ", windowStartTime='" + windowStartTime + '\'' +
                ", windowEndTime='" + windowEndTime + '\'' +
                ", currentWatermark='" + currentWatermark + '\'' +
                ", currentProcessingTime='" + currentProcessingTime + '\'' +
                '}';
    }
}
