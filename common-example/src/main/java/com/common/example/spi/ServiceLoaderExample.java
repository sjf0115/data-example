package com.common.example.spi;

import com.google.common.base.Objects;

import java.util.ServiceLoader;

/**
 * 功能：
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2021/8/17 下午10:06
 */
public class ServiceLoaderExample {
    public static void main(String[] args) {
        ServiceLoader<SPIService> spiServices = ServiceLoader.load(SPIService.class);
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        ServiceLoader<SPIService> spiServices2 = ServiceLoader.load(SPIService.class, contextClassLoader);
        if (Objects.equal(spiServices, spiServices2)) {
            System.out.println("same");
        } else {
            System.out.println("not same");
        }
    }
}
