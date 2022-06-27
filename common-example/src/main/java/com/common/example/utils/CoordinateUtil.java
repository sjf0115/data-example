package com.common.example.utils;

import com.common.example.bean.Coordinate;
import org.apache.commons.lang3.StringUtils;

/**
 * 功能：坐标工具类
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/6/27 下午3:04
 */
public class CoordinateUtil {
    // 经度最大绝对值
    public static double MAX_LON = 180;
    // 纬度最大绝对值
    public static double MAX_LAT = 90;

    /**
     * 全球坐标系转为Google坐标系
     *
     * @param wgs84Lat
     * @param wgs84Lgt
     * @return
     */
    public static Coordinate fromWGS84ToGCJ02(double wgs84Lat, double wgs84Lgt) {
        if (isOutOfChina(wgs84Lat, wgs84Lgt)) {
            return new Coordinate(wgs84Lat, wgs84Lgt);
        } else {
            double dLat = transformLat(wgs84Lgt - 105.0D, wgs84Lat - 35.0D);
            double dLgt = transformLgt(wgs84Lgt - 105.0D, wgs84Lat - 35.0D);
            double radLat = wgs84Lat / 180.0D * 3.141592653589793D;
            double magic = Math.sin(radLat);
            magic = 1.0D - 0.006693421622965943D * magic * magic;
            double sqrtMagic = Math.sqrt(magic);
            dLat = dLat * 180.0D / (6335552.717000426D / (magic * sqrtMagic) * 3.141592653589793D);
            dLgt = dLgt * 180.0D / (6378245.0D / sqrtMagic * Math.cos(radLat) * 3.141592653589793D);
            return new Coordinate(wgs84Lat + dLat, wgs84Lgt + dLgt);
        }
    }

    /**
     * 全球坐标系转为Google坐标系
     *
     * @param wgs84LatLgt
     * @return
     */
    public static Coordinate fromWGS84ToGCJ02(Coordinate wgs84LatLgt) {
        return fromWGS84ToGCJ02(wgs84LatLgt.x, wgs84LatLgt.y);
    }

    /**
     * 全球坐标系转为百度坐标系
     *
     * @param wgs84Lat
     * @param wgs84Lgt
     * @return
     */
    public static Coordinate fromWGS84ToBD09(double wgs84Lat, double wgs84Lgt) {
        return fromGCJ02ToBD09(fromWGS84ToGCJ02(wgs84Lat, wgs84Lgt));
    }

    /**
     * 全球坐标系转为百度坐标系
     *
     * @param wgs84LatLgt
     * @return
     */
    public static Coordinate fromWGS84ToBD09(Coordinate wgs84LatLgt) {
        return fromWGS84ToBD09(wgs84LatLgt.x, wgs84LatLgt.y);
    }

    /**
     * Google坐标系转为全球坐标系
     *
     * @param gcj02Lat
     * @param gcj02Lgt
     * @return
     */
    public static Coordinate fromGCJ02ToWGS84(double gcj02Lat, double gcj02Lgt) {
        if (isOutOfChina(gcj02Lat, gcj02Lgt)) {
            return new Coordinate(gcj02Lat, gcj02Lgt);
        } else {
            double dLat = transformLat(gcj02Lgt - 105.0D, gcj02Lat - 35.0D);
            double dLgt = transformLgt(gcj02Lgt - 105.0D, gcj02Lat - 35.0D);
            double radLat = gcj02Lat / 180.0D * 3.141592653589793D;
            double magic = Math.sin(radLat);
            magic = 1.0D - 0.006693421622965943D * magic * magic;
            double sqrtMagic = Math.sqrt(magic);
            dLat = gcj02Lat + dLat * 180.0D / (6335552.717000426D / (magic * sqrtMagic) * 3.141592653589793D);
            dLgt = gcj02Lgt + dLgt * 180.0D / (6378245.0D / sqrtMagic * Math.cos(radLat) * 3.141592653589793D);
            return new Coordinate(gcj02Lat * 2.0D - dLat, gcj02Lgt * 2.0D - dLgt);
        }
    }

    /**
     * Google坐标系转为全球坐标系
     *
     * @param gcj02LatLgt
     * @return
     */
    public static Coordinate fromGCJ02ToWGS84(Coordinate gcj02LatLgt) {
        return fromGCJ02ToWGS84(gcj02LatLgt.x, gcj02LatLgt.y);
    }

    /**
     * Google坐标系转为百度坐标系
     *
     * @param gcj02Lat
     * @param gcj02Lgt
     * @return
     */
    public static Coordinate fromGCJ02ToBD09(double gcj02Lat, double gcj02Lgt) {
        double z = Math.sqrt(gcj02Lgt * gcj02Lgt + gcj02Lat * gcj02Lat) + 2.0E-5D * Math.sin(gcj02Lat * 52.35987755982988D);
        double theta = Math.atan2(gcj02Lat, gcj02Lgt) + 3.0E-6D * Math.cos(gcj02Lgt * 52.35987755982988D);
        return new Coordinate(z * Math.sin(theta) + 0.006D, z * Math.cos(theta) + 0.0065D);
    }

    /**
     * Google坐标系转为百度坐标系
     *
     * @param gcj02LatLgt
     * @return
     */
    public static Coordinate fromGCJ02ToBD09(Coordinate gcj02LatLgt) {
        return fromGCJ02ToBD09(gcj02LatLgt.x, gcj02LatLgt.y);
    }

    /**
     * 百度坐标系转为Google坐标系
     *
     * @param bd09Lat
     * @param bd09Lgt
     * @return
     */
    public static Coordinate fromBD09ToGCJ02(double bd09Lat, double bd09Lgt) {
        double x = bd09Lgt - 0.0065D;
        double y = bd09Lat - 0.006D;
        double z = Math.sqrt(x * x + y * y) - 2.0E-5D * Math.sin(y * 52.35987755982988D);
        double theta = Math.atan2(y, x) - 3.0E-6D * Math.cos(x * 52.35987755982988D);
        return new Coordinate(z * Math.sin(theta), z * Math.cos(theta));
    }

    /**
     * 百度坐标系转为Google坐标系
     *
     * @param bd09LatLgt
     * @return
     */
    public static Coordinate fromBD09ToGCJ02(Coordinate bd09LatLgt) {
        return fromBD09ToGCJ02(bd09LatLgt.x, bd09LatLgt.y);
    }

    /**
     * 百度坐标系转为全球坐标系
     *
     * @param bd09Lat
     * @param bd09Lgt
     * @return
     */
    public static Coordinate fromBD09ToWGS84(double bd09Lat, double bd09Lgt) {
        return fromGCJ02ToWGS84(fromBD09ToGCJ02(bd09Lat, bd09Lgt));
    }

    /**
     * 百度坐标系转为全球坐标系
     *
     * @param bd09LatLgt
     * @return
     */
    public static Coordinate fromBD09ToWGS84(Coordinate bd09LatLgt) {
        return fromBD09ToWGS84(bd09LatLgt.x, bd09LatLgt.y);
    }

    private static boolean isOutOfChina(double wgs84Lat, double wgs84Lgt) {
        return wgs84Lat < 0.8293D || wgs84Lat > 55.8271D || wgs84Lgt < 72.004D || wgs84Lgt > 137.8347D;
    }

    private static boolean isOutOfChina(Coordinate wgs84LatLgt) {
        return isOutOfChina(wgs84LatLgt.x, wgs84LatLgt.y);
    }

    private static double transformLat(double x, double y) {
        return -100.0D + 2.0D * x + 3.0D * y + 0.2D * y * y + 0.1D * x * y + 0.2D * Math.sqrt(Math.abs(x)) + (20.0D * Math.sin(6.0D * x * 3.141592653589793D) + 20.0D * Math.sin(2.0D * x * 3.141592653589793D) + 20.0D * Math.sin(y * 3.141592653589793D) + 40.0D * Math.sin(y / 3.0D * 3.141592653589793D) + 160.0D * Math.sin(y / 12.0D * 3.141592653589793D) + 320.0D * Math.sin(y * 3.141592653589793D / 30.0D)) * 2.0D / 3.0D;
    }

    private static double transformLgt(double x, double y) {
        return 300.0D + x + 2.0D * y + 0.1D * x * x + 0.1D * x * y + 0.1D * Math.sqrt(Math.abs(x)) + (20.0D * Math.sin(6.0D * x * 3.141592653589793D) + 20.0D * Math.sin(2.0D * x * 3.141592653589793D) + 20.0D * Math.sin(x * 3.141592653589793D) + 40.0D * Math.sin(x / 3.0D * 3.141592653589793D) + 150.0D * Math.sin(x / 12.0D * 3.141592653589793D) + 300.0D * Math.sin(x / 30.0D * 3.141592653589793D)) * 2.0D / 3.0D;
    }

    /**
     * 坐标转换
     * @param lonStr 经度
     * @param latStr 纬度
     * @return
     */
    public static Coordinate fromLatAndLon(String latStr, String lonStr){
        if(StringUtils.isBlank(lonStr) || StringUtils.isBlank(latStr)){
            return null;
        }

        try{
            double lat = Double.parseDouble(latStr);
            double lon = Double.parseDouble(lonStr);
            if(Math.abs(lat) > MAX_LAT || Math.abs(lon) > MAX_LON){
                return null;
            }
            Coordinate coordinate = new Coordinate(lat, lon);
            return coordinate;
        }
        catch (Exception e){
            return null;
        }

    }

    public static void main(String[] args) {
        Coordinate coordinate = fromGCJ02ToWGS84(39.908735437817676, 116.39748584235535);
        System.out.println(coordinate.getX() + ", " + coordinate.getY());
    }
}
