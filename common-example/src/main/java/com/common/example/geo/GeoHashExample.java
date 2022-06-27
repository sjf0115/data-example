package com.common.example.geo;

import ch.hsr.geohash.GeoHash;

/**
 * 功能：GeoHash
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/6/25 下午3:46
 */
public class GeoHashExample {
    public static void main(String[] args) {
        double lat = 39.90875452232849;
        double lon = 116.39748622335468;

        GeoHash geoHash = GeoHash.withCharacterPrecision(lat, lon, 7);
        System.out.println(geoHash.significantBits());
        System.out.println(geoHash.toBase32());
    }
}
