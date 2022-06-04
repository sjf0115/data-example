package com.common.example.jmx.server;

import com.common.example.jmx.bean.Counter;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;

/**
 * 功能：Counter 资源管理
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/6/4 下午11:36
 */
public class CounterManagement {
    public static void main(String[] args) throws Exception {
        // 获取 MBean Server
        MBeanServer platformMBeanServer = ManagementFactory.getPlatformMBeanServer();
        // 创建 MBean
        Counter counter = new Counter();
        counter.setCounter(0);
        // 注册
        ObjectName objectName = new ObjectName("com.common.example.jmx:type=Counter, name=CounterMBean");
        platformMBeanServer.registerMBean(counter, objectName);
        // 防止退出
        while (true) {
            Thread.sleep(3000);
            System.out.println("[INFO] 休眠 3s ..............");
        }
    }
}
