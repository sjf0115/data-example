package com.common.example.utils;

import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 * 功能：时间工具类
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2022/9/2 下午10:52
 */
public class TimeUtil {

    public static String DATE_FORMAT = "yyyy-MM-dd";
    public static String FULL_DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";

    public static String getCurrentDate(){
        return getCurrentDate(DATE_FORMAT);
    }

    public static String getCurrentDate(String format){
        if(StringUtils.isBlank(format)){
            format = FULL_DATE_FORMAT;
        }
        DateTime now = new DateTime();
        return now.toString(format);
    }

    // 日期转 DateTime
    public static DateTime toDateTime(String date){
        return toDateTime(date, FULL_DATE_FORMAT);
    }

    // 日期转 DateTime
    public static DateTime toDateTime(String date, String format){
        DateTimeFormatter dateTimeFormatter = DateTimeFormat.forPattern(format);
        DateTime dateTime = DateTime.parse(date, dateTimeFormatter);
        return dateTime;
    }

    public static DateTime plusYears(String date, String format, int years) {
        DateTime dateTime = toDateTime(date, format);
        return dateTime.plusYears(years);
    }

    public static DateTime plusDays(String date, String format, int days) {
        DateTime dateTime = toDateTime(date, format);
        return dateTime.plusDays(days);
    }
}
