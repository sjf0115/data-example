package com.common.example.jmx.server;

import com.common.example.bean.ResourceItem;
import com.common.example.jmx.bean.ResourceX;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;

/**
 * 功能：MXBeanServer 注册 MBean
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/6/2 下午4:41
 */
public class ResourceXManagement {
    public static void main(String[] args) throws Exception {
        // 获取 MBean Server
        MBeanServer platformMBeanServer = ManagementFactory.getPlatformMBeanServer();

        // 创建 MXBean
        ResourceX resource = new ResourceX();
        resource.addItem(new ResourceItem("a", 1));
        resource.addItem(new ResourceItem("b", 2));
        resource.addItem(new ResourceItem("c", 3));

        // 注册 MXBean 到 MBean Server
        ObjectName objectName = new ObjectName("com.common.example.jmx:type=ResourceX, name=CustomResourceMXBean");
        platformMBeanServer.registerMBean(resource, objectName);

        // 防止退出
        while (true) {
            Thread.sleep(3000);
            System.out.println("[INFO] 休眠 3s ..............");
        }
    }
}
