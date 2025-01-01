package com.spark.example.streaming.connector.receiver;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;

/**
 * 功能：自定义 Receiver
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2025/1/1 18:45
 */
public class CustomSourceReceiver extends Receiver<String> {

    public CustomSourceReceiver() {
        super(StorageLevel.MEMORY_AND_DISK_2());
    }

    @Override
    public void onStart() {

    }

    @Override
    public void onStop() {

    }
}
