package com.common.example.jmx.bean;

import java.lang.management.*;
import java.util.List;

/**
 * 功能：JMX MXBean 简单示例
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/6/2 下午2:56
 */
public class MXBeanSimpleExample {
    public static void main(String[] args) {
        operatingSystemMXBean();
        //compilationMXBean();
        //memoryMXBean();
        //garbageCollectorMXBeans();
    }

    // 操作系统 MXBean
    private static void operatingSystemMXBean() {
        OperatingSystemMXBean osMXBean = ManagementFactory.getOperatingSystemMXBean();
        // 操作系统名称
        String osName = osMXBean.getName();
        // 操作系统版本
        String osVersion = osMXBean.getVersion();
        // 操作系统处理器个数
        int processors = osMXBean.getAvailableProcessors();
        // 型号
        String arch = osMXBean.getArch();
        // 平均系统负载
        double systemLoadAverage = osMXBean.getSystemLoadAverage();
        String result = String.format("操作系统：%s，版本：%s，处理器：%d 个, 型号: %s, 平均系统负载: %f",
                osName, osVersion, processors, arch, systemLoadAverage);
        System.out.println(result);
    }

    // 编译系统 MXBean
    private static void compilationMXBean() {
        CompilationMXBean cMXBean = ManagementFactory.getCompilationMXBean();
        // 名称
        String name = cMXBean.getName();
        System.out.println("编译系统：" + name);
    }

    // 内存 MXBean
    private static void memoryMXBean() {
        MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
        // 堆内存
        MemoryUsage heapMemoryUsage = memoryMXBean.getHeapMemoryUsage();
        long max = heapMemoryUsage.getMax();
        long used = heapMemoryUsage.getUsed();
        String result = String.format("堆内存：%dMB/%dMB", used / 1024 / 1024, max / 1024 / 1024);
        System.out.println(result);
    }

    private static void garbageCollectorMXBeans() {
        List<GarbageCollectorMXBean> gcMXBeans = ManagementFactory.getGarbageCollectorMXBeans();
        for (GarbageCollectorMXBean bean : gcMXBeans) {
            String collectorName = bean.getName();
            long collectionCount = bean.getCollectionCount();
            long collectionTime = bean.getCollectionTime();
            String result = String.format("垃圾收集器：%s，收集次数：%d 次, 收集时间: %d 毫秒",
                    collectorName, collectionCount, collectionTime);
            System.out.println(result);
        }
    }
}
