package com.common.example.utils;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.parquet.io.api.Binary;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import java.io.*;
import java.util.Objects;

/**
 * 功能：RoaringBitmap 工具类
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2022/11/6 下午5:02
 */
public class RoaringBitmapUtil {

    /**
     * Bitmap 转换为 byte[]
     * @param bitmap
     * @return
     */
    public static byte[] parseByteArray(Roaring64NavigableMap bitmap) {
        if (Objects.equals(bitmap, null)){
            return null;
        }
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);
        try {
            bitmap.runOptimize();
            bitmap.serialize(dos);
            dos.close();
        }
        catch (IOException e) {
            throw new RuntimeException("Exception happen when serialize bitmap:" + e.getMessage());
        }
        return bos.toByteArray();
    }

    /**
     * Bitmap 转换为 BytesWritable (Hadoop)
     * @param bitmap
     * @return
     */
    public static BytesWritable parseBytesWritable(Roaring64NavigableMap bitmap) {
        if (Objects.equals(bitmap, null)){
            return null;
        }
        byte[] bytes = parseByteArray(bitmap);
        return new BytesWritable(bytes);
    }

    /**
     * Bitmap 转换为 String
     * @param bitmap
     * @return
     */
    public static String parseString(Roaring64NavigableMap bitmap) {
//        if (Objects.equals(bitmap, null)){
//            return null;
//        }
//        ByteArrayOutputStream bos = new ByteArrayOutputStream();
//        DataOutputStream dos = new DataOutputStream(bos);
//        String value;
//        try {
//            bitmap.runOptimize();
//            bitmap.serialize(dos);
//            dos.close();
//            value = bos.toString("UTF-8");
//        }
//        catch (IOException e) {
//            throw new RuntimeException("Exception happen when serialize bitmap:" + e.getMessage());
//        }
        byte[] bytes = parseByteArray(bitmap);
        return new String(bytes);
    }

    /**
     * Bitmap 转换为 Text
     * @param bitmap
     * @return
     */
    public static Text parseText(Roaring64NavigableMap bitmap) {
        String value = parseString(bitmap);
        return new Text(value);
    }

    /**
     * Binary 转换为 Bitmap
     * @param binary
     * @return
     */
    public static Roaring64NavigableMap parseBitmap(Binary binary) {
        if(Objects.equals(binary, null)) {
            return null;
        }
        return parseBitmap(binary.getBytes());
    }

    /**
     * Hadoop Writable 转换为 Bitmap
     * @param writable
     * @return
     */
    public static Roaring64NavigableMap parseBitmap(Writable writable) {
        if(Objects.equals(writable, null)) {
            return null;
        }
        BytesWritable ret = (BytesWritable) writable;
        return parseBitmap(ret.getBytes());
    }

    /**
     * String 转换为 Bitmap
     * @param str
     * @return
     */
    public static Roaring64NavigableMap parseBitmap(String str) {
        if(Objects.equals(str, null)) {
            return null;
        }
        return parseBitmap(str.getBytes());
    }

    /**
     * Byte数组 转换为 Bitmap
     * @param byteArray
     * @return
     */
    public static Roaring64NavigableMap parseBitmap(byte[] byteArray) {
        if(Objects.equals(byteArray, null)) {
            return null;
        }
        Roaring64NavigableMap bitmap = new Roaring64NavigableMap();
        ByteArrayInputStream bis = new ByteArrayInputStream(byteArray);
        DataInputStream dis = new DataInputStream(bis);
        try {
            bitmap.deserialize(dis);
            bis.close();
            dis.close();
        } catch (IOException e) {
            e.printStackTrace();
            //throw new RuntimeException("Exception happen when deserialize bitmap:" + e.getMessage());
        }
        return bitmap;
    }
}