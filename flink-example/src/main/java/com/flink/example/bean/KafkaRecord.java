package com.flink.example.bean;

/**
 * 功能：Kafka 记录
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2021/9/12 下午6:13
 */
public class KafkaRecord<K, V> {
    private String topic;
    private int partition;
    private long offset;
    private long timestamp;
    private int serializedKeySize;
    private int serializedValueSize;
    private K key;
    private V value;

    public KafkaRecord(String topic, int partition, long offset, long timestamp, int serializedKeySize, int serializedValueSize, K key, V value) {
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.timestamp = timestamp;
        this.serializedKeySize = serializedKeySize;
        this.serializedValueSize = serializedValueSize;
        this.key = key;
        this.value = value;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public int getPartition() {
        return partition;
    }

    public void setPartition(int partition) {
        this.partition = partition;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public int getSerializedKeySize() {
        return serializedKeySize;
    }

    public void setSerializedKeySize(int serializedKeySize) {
        this.serializedKeySize = serializedKeySize;
    }

    public int getSerializedValueSize() {
        return serializedValueSize;
    }

    public void setSerializedValueSize(int serializedValueSize) {
        this.serializedValueSize = serializedValueSize;
    }

    public K getKey() {
        return key;
    }

    public void setKey(K key) {
        this.key = key;
    }

    public V getValue() {
        return value;
    }

    public void setValue(V value) {
        this.value = value;
    }
}
