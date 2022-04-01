package com.flink.example.bean;

/**
 * 功能：WordCount
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/4/1 下午9:24
 */
public class WordCount {
    // 单词
    private String word;
    // 频次
    private Long frequency;

    public WordCount() {
    }

    public WordCount(String word, Long frequency) {
        this.word = word;
        this.frequency = frequency;
    }

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public Long getFrequency() {
        return frequency;
    }

    public void setFrequency(Long frequency) {
        this.frequency = frequency;
    }
}
