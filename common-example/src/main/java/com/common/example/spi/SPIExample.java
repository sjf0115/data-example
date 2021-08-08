package com.common.example.spi;

import java.util.Iterator;
import java.util.ServiceLoader;

/**
 * 功能：SPI 示例
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2021/8/7 下午1:32
 */
public class SPIExample {
    public static void main(String[] args) {
        ServiceLoader<SPIService> spiServices = ServiceLoader.load(SPIService.class);
        Iterator iterator = spiServices.iterator();
        while (iterator.hasNext()){
            SPIService spiService = (SPIService)  iterator.next();
            spiService.print();
        }
    }
}
