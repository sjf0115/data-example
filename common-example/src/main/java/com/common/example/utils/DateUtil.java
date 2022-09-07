package com.common.example.utils;

import org.apache.commons.lang3.StringUtils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Objects;

/**
 * 时间工具类
 * Created by wy on 2021/1/5.
 */
public class DateUtil {

    public static String DATE_FORMAT = "yyyy-MM-dd";
    public static String FULL_DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";

    /**
     * 获取当前时间
     * @return
     */
    public static String currentDate(){
        SimpleDateFormat df = new SimpleDateFormat(FULL_DATE_FORMAT);
        return df.format(System.currentTimeMillis());
    }

    /**
     * 时间字符串转时间戳
     * @param dateStr 字符串时间
     * @param format 时间格式
     * @return
     */
    public static Long date2TimeStamp(String dateStr, String format) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat(format);
        return sdf.parse(dateStr).getTime();
    }

    /**
     * 时间字符串转时间戳
     * @param dateStr
     * @return
     * @throws ParseException
     */
    public static Long date2TimeStamp(String dateStr) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat(FULL_DATE_FORMAT);
        return sdf.parse(dateStr).getTime();
    }

    /**
     * 时间 格式化 yyyy-MM-dd
     *
     * @param dateStr 字符串时间
     * @param format 时间格式
     * @return
     */
    public static String dateFormat(String dateStr, String format) throws ParseException {
        Date date = new SimpleDateFormat(format).parse(dateStr);
        String dateFormat = new SimpleDateFormat("yyyy-MM-dd").format(date);
        return dateFormat;
    }

    /**
     * 时间 格式化
     *
     * @param dateStr 时间
     * @param sourceFormat 源时间格式
     * @param targetFormat 目标时间格式
     * @return
     */
    public static String dateFormat(String dateStr, String sourceFormat, String targetFormat) throws ParseException {
        Date date = new SimpleDateFormat(sourceFormat).parse(dateStr);
        String dateFormat = new SimpleDateFormat(targetFormat).format(date);
        return dateFormat;
    }

    /**
     * 时间 格式化
     *
     * @param dateSource
     * @param format 目标格式化
     * @return
     */
    public static String dateFormat(Date dateSource, String format) {
        if (Objects.equals(dateSource, null) || StringUtils.isBlank(format)) {
            return null;
        }
        SimpleDateFormat sdf = new SimpleDateFormat(format);
        String dateFormat = sdf.format(dateSource);
        return dateFormat;
    }

    /**
     * 时间戳转字符串时间
     * @param timeStamp
     * @param formats
     * @return
     */
    public static String timeStamp2Date(Long timeStamp, String formats){
        String date = new SimpleDateFormat(formats).format(new Date(timeStamp));
        return date;
    }

    /**
     * 时间戳转字符串时间
     * @param timeStamp
     * @return
     */
    public static String timeStamp2Date(Long timeStamp){
        String date = new SimpleDateFormat(FULL_DATE_FORMAT).format(new Date(timeStamp));
        return date;
    }

    /**
     * 星期
     * @param date
     * @return
     * @throws ParseException
     */
    public static String getWeek(String date) throws ParseException {
        SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
        Date dt = format.parse(date);
        return  getWeek(dt);
    }

    /**
     * 星期
     * @param date
     * @return
     */
    public static String getWeek(Date date) {
        String[] weekDays = {"星期天", "星期一", "星期二", "星期三", "星期四", "星期五", "星期六"};
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        int w = cal.get(Calendar.DAY_OF_WEEK) - 1;
        if (w < 0){
            w = 0;
        }
        return weekDays[w];
    }

}
