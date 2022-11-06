package com.common.example.utils;

import com.google.common.base.Objects;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.BytesWritable;
import org.apache.parquet.io.api.Binary;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

/**
 * 功能：BitmapFunction
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2022/11/6 下午5:34
 */
public class BitmapFunction {
    /**
     * Bitmap 与
     * @param b1
     * @param b2
     * @return
     */
    public static BytesWritable bitmapAnd(Binary b1, Binary b2){
        if (Objects.equal(b1, null) || Objects.equal(b2, null)) {
            return null;
        }
        Roaring64NavigableMap retBitmap = RoaringBitmapUtil.parseBitmap(b1);
        Roaring64NavigableMap tmpBitmap = RoaringBitmapUtil.parseBitmap(b2);
        retBitmap.and(tmpBitmap);
        return RoaringBitmapUtil.parseBytesWritable(retBitmap);
    }

    /**
     * Bitmap 与
     * @param b1
     * @param b2
     * @return
     */
    public static byte[] bitmapAnd(byte[] b1, byte[] b2){
        if (Objects.equal(b1, null) || Objects.equal(b2, null)) {
            return null;
        }
        Roaring64NavigableMap retBitmap = RoaringBitmapUtil.parseBitmap(b1);
        Roaring64NavigableMap tmpBitmap = RoaringBitmapUtil.parseBitmap(b2);
        retBitmap.and(tmpBitmap);
        return RoaringBitmapUtil.parseByteArray(retBitmap);
    }

    /**
     * Bitmap 与
     * @param b1
     * @param b2
     * @return
     */
    public static BytesWritable bitmapAnd(BytesWritable b1, BytesWritable b2){
        if (Objects.equal(b1, null) || Objects.equal(b2, null)) {
            return null;
        }
        Roaring64NavigableMap retBitmap = RoaringBitmapUtil.parseBitmap(b1);
        Roaring64NavigableMap tmpBitmap = RoaringBitmapUtil.parseBitmap(b2);
        retBitmap.and(tmpBitmap);
        return RoaringBitmapUtil.parseBytesWritable(retBitmap);
    }

    /**
     * Bitmap 与
     * @param args
     * @return
     */
    public static byte[] bitmapAnd(byte[]... args){
        if (Objects.equal(args, null)) {
            return null;
        }
        int size = args.length;
        byte[] baseBytes = args[0];
        if (size == 1) {
            return baseBytes;
        }
        Roaring64NavigableMap baseBitmap = RoaringBitmapUtil.parseBitmap(baseBytes);
        for (int index = 1; index < size; index++) {
            if (Objects.equal(args[index], null)) {
                return null;
            }
            baseBitmap.and(RoaringBitmapUtil.parseBitmap(args[index]));
        }
        return RoaringBitmapUtil.parseByteArray(baseBitmap);
    }

    /**
     * Bitmap 与
     * @param args
     * @return
     */
    public static BytesWritable bitmapAnd(BytesWritable... args){
        if (Objects.equal(args, null)) {
            return null;
        }
        int size = args.length;
        BytesWritable baseBytes = args[0];
        if (size == 1) {
            return baseBytes;
        }
        Roaring64NavigableMap baseBitmap = RoaringBitmapUtil.parseBitmap(baseBytes);
        for (int index = 1; index < size; index++) {
            if (Objects.equal(args[index], null)) {
                return null;
            }
            baseBitmap.and(RoaringBitmapUtil.parseBitmap(args[index]));
        }
        return RoaringBitmapUtil.parseBytesWritable(baseBitmap);
    }

    /**
     * Bitmap 或
     * @param b1
     * @param b2
     * @return
     */
    public static BytesWritable bitmapOr(Binary b1, Binary b2) {
        Roaring64NavigableMap retBitmap = RoaringBitmapUtil.parseBitmap(b1);
        Roaring64NavigableMap tmpBitmap = RoaringBitmapUtil.parseBitmap(b2);
        if (!Objects.equal(tmpBitmap, null)) {
            retBitmap.or(tmpBitmap);
        }
        return RoaringBitmapUtil.parseBytesWritable(retBitmap);
    }

    /**
     * Bitmap 或
     * @param b1
     * @param b2
     * @return
     */
    public static BytesWritable bitmapOr(BytesWritable b1, BytesWritable b2){
        if (Objects.equal(b1, null)) {
            return b2;
        }
        if (Objects.equal(b2, null)) {
            return b1;
        }
        Roaring64NavigableMap retBitmap = RoaringBitmapUtil.parseBitmap(b1);
        Roaring64NavigableMap tmpBitmap = RoaringBitmapUtil.parseBitmap(b2);
        retBitmap.or(tmpBitmap);
        return RoaringBitmapUtil.parseBytesWritable(retBitmap);
    }

    /**
     * Bitmap 或
     * @param b1
     * @param b2
     * @return
     */
    public static byte[] bitmapOr(byte[] b1, byte[] b2) {
        if (Objects.equal(b1, null)) {
            return b2;
        }
        if (Objects.equal(b2, null)) {
            return b1;
        }
        Roaring64NavigableMap retBitmap = RoaringBitmapUtil.parseBitmap(b1);
        Roaring64NavigableMap tmpBitmap = RoaringBitmapUtil.parseBitmap(b2);
        retBitmap.or(tmpBitmap);
        return RoaringBitmapUtil.parseByteArray(retBitmap);
    }

    /**
     * Bitmap 或
     * @param args
     * @return
     */
    public static byte[] bitmapOr(byte[]... args) {
        if (Objects.equal(args, null)) {
            return null;
        }
        int size = args.length;
        if (size == 1) {
            return args[0];
        }
        Roaring64NavigableMap baseBitmap = new Roaring64NavigableMap();
        for (int index = 0; index < size; index++) {
            if (Objects.equal(args[index], null)) {
                continue;
            }
            baseBitmap.or(RoaringBitmapUtil.parseBitmap(args[index]));
        }
        return RoaringBitmapUtil.parseByteArray(baseBitmap);
    }

    /**
     * Bitmap 或操作
     * @param args
     * @return
     */
    public static BytesWritable bitmapOr(BytesWritable... args){
        if (Objects.equal(args, null)) {
            return null;
        }
        int size = args.length;
        BytesWritable baseBytes = args[0];
        if (size == 1) {
            return baseBytes;
        }
        Roaring64NavigableMap baseBitmap = new Roaring64NavigableMap();
        for (int index = 0; index < size; index++) {
            if (Objects.equal(args[index], null)) {
                continue;
            }
            baseBitmap.or(RoaringBitmapUtil.parseBitmap(args[index]));
        }
        return RoaringBitmapUtil.parseBytesWritable(baseBitmap);
    }

    /**
     * Bitmap 基数
     * @param b1
     * @return
     */
    public static Long bitmapCount(Binary b1) {
        if (Objects.equal(b1, null)) {
            return 0L;
        }
        Roaring64NavigableMap retBitmap = RoaringBitmapUtil.parseBitmap(b1);
        return retBitmap.getLongCardinality();
    }

    /**
     * Bitmap 基数
     * @param str
     * @return
     */
    public static Long bitmapCount(String str) {
        if (StringUtils.isBlank(str)) {
            return 0L;
        }
        Roaring64NavigableMap retBitmap = RoaringBitmapUtil.parseBitmap(str);
        return retBitmap.getLongCardinality();
    }

    /**
     * Bitmap 基数
     * @param bytes
     * @return
     */
    public static Long bitmapCount(byte[] bytes) {
        if (Objects.equal(bytes, null)) {
            return 0L;
        }
        Roaring64NavigableMap retBitmap = RoaringBitmapUtil.parseBitmap(bytes);
        return retBitmap.getLongCardinality();
    }

    /**
     * Bitmap 基数
     * @param bytesWritable
     * @return
     */
    public static Long bitmapCount(BytesWritable bytesWritable) {
        if (Objects.equal(bytesWritable, null)) {
            return 0L;
        }
        Roaring64NavigableMap retBitmap = RoaringBitmapUtil.parseBitmap(bytesWritable);
        return retBitmap.getLongCardinality();
    }

    /**
     * Bitmap 与操作后返回基数
     * @param args
     * @return
     */
    public static Long bitmapAndCount(byte[]... args) {
        byte[] bytes = bitmapAnd(args);
        Long result = bitmapCount(bytes);
        return result;
    }

    /**
     * Bitmap 与操作后返回基数
     * @param args
     * @return
     */
    public static Long bitmapAndCount(BytesWritable... args) {
        org.apache.hadoop.io.BytesWritable bytes = bitmapAnd(args);
        Long result = bitmapCount(bytes);
        return result;
    }

    /**
     * Bitmap 或操作后返回基数
     * @param args
     * @return
     */
    public static Long bitmapOrCount(byte[]... args) {
        byte[] bytes = bitmapOr(args);
        Long result = bitmapCount(bytes);
        return result;
    }

    /**
     * Bitmap 或操作后返回基数
     * @param args
     * @return
     */
    public static Long bitmapOrCount(BytesWritable... args) {
        org.apache.hadoop.io.BytesWritable bytes = bitmapOr(args);
        Long result = bitmapCount(bytes);
        return result;
    }

    /**
     * Bitmap 分桶索引 默认每个分桶最大存储5000w用户
     * @param uid
     * @return
     */
//    public static Long bitmapBucketIndex(String uid) {
//        Long index = bitmapBucketIndex(uid, 50000000L);
//        return index;
//    }

    /**
     * Bitmap 分桶索引
     * @param uid 用户uid
     * @param max 每个分桶最大存储用户量
     * @return
     */
//    public static Long bitmapBucketIndex(String uid, Long max) {
//        Long uidLong = RoaringBitmapUtil.parseLong(uid);
//        Long index;
//        /*if(UidUtils.isSGPUid(uidLong)){
//            index = (uidLong - SGP_UID_START_START) / max * -1;
//        }
//        else {
//            index = (uidLong - XUEXI_UID_START_START) / max;
//        }*/
//        index = (uidLong - XUEXI_UID_START_START) / max;
//        return index;
//    }

    /**
     * Bitmap 分桶存储值 默认每个分桶最大存储5000w用户
     * @param uid 用户uid
     * @return
     */
//    public static Long bitmapBucketValue(String uid) {
//        Long value =  bitmapBucketValue(uid, 50000000L);
//        return value;
//    }

    /**
     * Bitmap 分桶存储值
     * @param uid 用户uid
     * @param max 每个分桶最大存储用户量
     * @return
     */
//    public static Long bitmapBucketValue(String uid, Long max) {
//        Long uidLong = RoaringBitmapUtil.parseLong(uid);
//        Long value;
//        /*if(UidUtils.isSGPUid(uidLong)) {
//            value = (uidLong - SGP_UID_START_START) % max * -1;
//        }
//        else {
//            value =  (uidLong - XUEXI_UID_START_START) % max;
//        }*/
//        value =  (uidLong - XUEXI_UID_START_START) % max;
//        return value;
//    }

    /**
     * Bitmap 是否包含
     * @param bytes
     * @param uid
     * @return
     */
    public static boolean bitmapContain(BytesWritable bytes, Long uid) {
        if (Objects.equal(bytes, null)) {
            return false;
        }
        Roaring64NavigableMap bitmap = RoaringBitmapUtil.parseBitmap(bytes);
        boolean isContain = bitmap.contains(uid);
        return isContain;
    }

    /**
     * 构造 Bitmap
     * @param uids
     * @return
     */
    public static byte[] bitmapOf(String uids) {
        if (StringUtils.isBlank(uids)) {
            return null;
        }
        return bitmapOf(uids, ",");
    }

    /**
     * 构造 Bitmap
     * @param uids
     * @param sep
     * @return
     */
    public static byte[] bitmapOf(String uids, String sep) {
        if (StringUtils.isBlank(uids)) {
            return null;
        }
        if (StringUtils.isBlank(sep)) {
            sep = ",";
        }
        String[] params = uids.split(sep);
        Roaring64NavigableMap bitmap = new Roaring64NavigableMap();
        for (String uid : params) {
            bitmap.addLong(Long.parseLong(uid));
        }
        byte[] bytes = RoaringBitmapUtil.parseByteArray(bitmap);
        return bytes;
    }
}
