package com.flink.example.stream.window;

import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;

/**
 * 功能：自定义 Evictor
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2021/9/4 下午8:10
 */
public class CustomEvictorExample {


    private static class CustomEvictor<W extends Window> implements Evictor<Object, W> {

        @Override
        public void evictBefore(Iterable<TimestampedValue<Object>> elements, int size, W window, EvictorContext evictorContext) {

        }

        @Override
        public void evictAfter(Iterable<TimestampedValue<Object>> elements, int size, W window, EvictorContext evictorContext) {

        }
    }

}
