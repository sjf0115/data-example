package com.common.example.bean;
/**
 * 功能：WordCount
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/4/5 下午8:07
 */
public class WordCount {
    // 单词
    private String word;
    // 频次
    private long frequency;

    public WordCount() {
    }

    public WordCount(String word, long frequency) {
        this.word = word;
        this.frequency = frequency;
    }

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public long getFrequency() {
        return frequency;
    }

    public void setFrequency(Long frequency) {
        this.frequency = frequency;
    }

    @Override
    public String toString() {
        return "WordCount{" +
                "word='" + word + '\'' +
                ", frequency=" + frequency +
                '}';
    }
}
