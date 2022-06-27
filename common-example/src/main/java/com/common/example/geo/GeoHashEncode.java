package com.common.example.geo;
/**
 * 功能：经纬度 GeoHash 编码
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/6/25 下午10:07
 */
public class GeoHashEncode {

    public static void main(String[] args) {
        double longitude = 116.39124226796913;
        double latitude = 39.90733194004775;
        double[] longitudeRange = new double[]{-180.0D, 180.0D};
        double[] latitudeRange = new double[]{-90.0D, 90.0D};

        int precision = 18;
        String lonEncode = encode(precision, longitude, longitudeRange);
        System.out.println("Lon: " + lonEncode);

        String latEncode = encode(precision, latitude, latitudeRange);
        System.out.println("Lat: " + latEncode);
    }

    private static String encode(int precision, double value, double[] range) {
        int index = 1;
        StringBuilder result = new StringBuilder();
        while (index <= precision) {
            int code;
            double min = range[0];
            double max = range[1];
            double mid = (min + max) / 2.0D;
            if(value >= mid) {
                range[0] = mid;
                code = 1;
            } else {
                range[1] = mid;
                code = 0;
            }
            result.append(code);
            System.out.println(index+ "|" + code + "|" + min + "|" + mid + "|" + max);
            index++;
        }
        return result.toString();
    }


}
