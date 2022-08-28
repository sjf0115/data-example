package com.flink.example.stream.window.window;

import com.common.example.utils.DateUtil;
import org.apache.flink.api.common.time.Time;

import java.text.ParseException;

/**
 * 功能：窗口开始时间
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2021/9/5 下午12:42
 */
public class WindowStartExample {

    private static long getWindowStartWithOffset(long timestamp, long offset, long windowSize) {
        return timestamp - (timestamp - offset + windowSize) % windowSize;
    }

    private static String windowStart(String eventTime, long offset, long windowSize) throws ParseException {
        Long timestamp = DateUtil.date2TimeStamp(eventTime);
        Long startTimestamp = getWindowStartWithOffset(timestamp, offset, windowSize);
        String startTime = DateUtil.timeStamp2Date(startTimestamp);
        String result = "WindowSize: " + windowSize +  ", EventTime: [" + eventTime + "|" + timestamp + "], WindowStart: [" + startTime + "|" + startTimestamp + "]";
        return result;
    }

    public static void main(String[] args) throws ParseException {
        // 1. 秒级窗口
        // 窗口大小 单位为秒 30秒一个窗口
        long windowSize = Time.seconds(30).toMilliseconds(); // 30000L
        // 滚动窗口 offset = 0L
        long offset = 0L;
        System.out.println("秒级窗口");
        System.out.println(windowStart("2021-08-30 12:07:00", offset, windowSize)); // 2021-08-30 12:07:00
        System.out.println(windowStart("2021-08-30 12:07:15", offset, windowSize)); // 2021-08-30 12:07:00
        System.out.println(windowStart("2021-08-30 12:07:29", offset, windowSize)); // 2021-08-30 12:07:00
        System.out.println(windowStart("2021-08-30 12:07:30", offset, windowSize)); // 2021-08-30 12:07:30
        System.out.println(windowStart("2021-08-30 12:07:33", offset, windowSize)); // 2021-08-30 12:07:30

        // 2. 分钟级窗口 单位为秒 1分钟一个窗口
        windowSize = Time.minutes(1).toMilliseconds(); // 60000L
        System.out.println("分钟级窗口");
        System.out.println(windowStart("2021-08-30 12:07:00", offset, windowSize)); // 2021-08-30 12:07:00
        System.out.println(windowStart("2021-08-30 12:07:59", offset, windowSize)); // 2021-08-30 12:07:00
        System.out.println(windowStart("2021-08-30 12:08:00", offset, windowSize)); // 2021-08-30 12:08:00
        System.out.println(windowStart("2021-08-30 12:08:15", offset, windowSize)); // 2021-08-30 12:08:00
        System.out.println(windowStart("2021-08-30 12:09:59", offset, windowSize)); // 2021-08-30 12:09:00

        // 3. 小时级窗口 单位为秒 1小时一个窗口
        windowSize = Time.hours(1).toMilliseconds(); // 3600000L
        System.out.println("小时级窗口");
        System.out.println(windowStart("2021-08-30 12:00:00", offset, windowSize)); // 2021-08-30 12:00:00
        System.out.println(windowStart("2021-08-30 12:23:59", offset, windowSize)); // 2021-08-30 12:00:00
        System.out.println(windowStart("2021-08-30 13:00:01", offset, windowSize)); // 2021-08-30 13:00:00
        System.out.println(windowStart("2021-08-30 14:01:15", offset, windowSize)); // 2021-08-30 14:00:00

        // 4. 天级窗口 单位为秒 1天一个窗口
        windowSize = Time.days(1).toMilliseconds(); // 86400000
        System.out.println("天级窗口");
        System.out.println(windowStart("2021-08-30 00:00:01", offset, windowSize)); // 2021-08-29 08:00:00
        System.out.println(windowStart("2021-08-30 07:23:59", offset, windowSize)); // 2021-08-29 08:00:00
        System.out.println(windowStart("2021-08-30 08:00:00", offset, windowSize)); // 2021-08-30 08:00:00
        System.out.println(windowStart("2021-08-30 09:01:15", offset, windowSize)); // 2021-08-30 08:00:00
        System.out.println(windowStart("2021-08-31 07:23:59", offset, windowSize)); // 2021-08-30 08:00:00
        System.out.println(windowStart("2021-08-31 08:00:00", offset, windowSize)); // 2021-08-31 08:00:00

        // 4. 天级窗口 单位为秒 1天一个窗口 偏移量-8个小时
        windowSize = Time.days(1).toMilliseconds(); // 86400000
        offset = Time.hours(-8).toMilliseconds();
        System.out.println("天级窗口");
        System.out.println(windowStart("2021-08-30 00:00:01", offset, windowSize)); // 2021-08-30 00:00:00
        System.out.println(windowStart("2021-08-30 07:23:59", offset, windowSize)); // 2021-08-30 00:00:00
        System.out.println(windowStart("2021-08-30 08:00:00", offset, windowSize)); // 2021-08-30 00:00:00
        System.out.println(windowStart("2021-08-30 09:01:15", offset, windowSize)); // 2021-08-30 00:00:00
        System.out.println(windowStart("2021-08-31 07:23:59", offset, windowSize)); // 2021-08-31 00:00:00
        System.out.println(windowStart("2021-08-31 08:00:00", offset, windowSize)); // 2021-08-31 00:00:00
    }
}
