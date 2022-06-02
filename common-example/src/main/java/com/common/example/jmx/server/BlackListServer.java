package com.common.example.jmx.server;

import com.common.example.jmx.bean.BlackList;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;
import java.net.InetSocketAddress;
import java.net.Socket;

/**
 * 功能：黑名单过滤
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/6/2 下午10:59
 */
public class BlackListServer {
    public static void main(String[] args) throws Exception {
        String hostname = "localhost";
        int port = 9000;
        // 获取 MBean Server
        MBeanServer platformMBeanServer = ManagementFactory.getPlatformMBeanServer();

        // 创建 MBean 初始黑名单用户为 a 和 b
        BlackList blackList = new BlackList();
        blackList.addBlackItem("a");
        blackList.addBlackItem("b");

        // 注册
        ObjectName objectName = new ObjectName("com.common.example.jmx:type=BlackList, name=BlackListMBean");
        platformMBeanServer.registerMBean(blackList, objectName);

        // 循环接收
        while (true) {
            // 简单从 Socket 接收字符串模拟接收到的用户Id
            try (Socket socket = new Socket()) {
                socket.connect(new InetSocketAddress(hostname, port), 0);
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {
                    char[] buffer = new char[8012];
                    int bytes;
                    while ((bytes = reader.read(buffer)) != -1) {
                        String result = new String(buffer, 0, bytes);
                        String uid = result;
                        // 去掉换行符
                        if (result.endsWith("\n")) {
                            uid = result.substring(0, result.length() - 1);
                        }
                        if (blackList.contains(uid)) {
                            System.out.println("[INFO] uid " + uid + " is in black list");
                        } else {
                            System.out.println("[INFO] uid " + uid + " is not in black list");
                        }
                    }
                }
            }
            Thread.sleep(3000);
            System.out.println("[INFO] 休眠 3s ..............");
        }
    }
}
