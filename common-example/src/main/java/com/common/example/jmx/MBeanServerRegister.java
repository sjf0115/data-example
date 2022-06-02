package com.common.example.jmx;

import javax.management.*;
import java.lang.management.ManagementFactory;

/**
 * 功能：MBeanServer 注册 MBean
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/6/2 下午4:41
 */
public class MBeanServerRegister {
    public static void main(String[] args) throws Exception {
        // 获取 MBean Server
        MBeanServer platformMBeanServer = ManagementFactory.getPlatformMBeanServer();

        // 创建 MBean
        Resource resource = new Resource();
        resource.addItem("item_1");
        resource.addItem("item_2");
        resource.addItem("item_3");

        // 注册
        ObjectName objectName = new ObjectName("com.common.example.jmx:type=Resource, name=CustomResourceMBean");
        platformMBeanServer.registerMBean(resource, objectName);

        // 防止退出
        while (true) {
            Thread.sleep(3000);
            System.out.println("[INFO] 休眠 3s ..............");
        }
    }
}
