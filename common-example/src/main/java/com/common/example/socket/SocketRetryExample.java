package com.common.example.socket;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.Socket;

/**
 * 功能：带重试的 Socket 连接
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/5/29 下午4:15
 */
public class SocketRetryExample {
    public static void main(String[] args) throws IOException, InterruptedException {
        String hostname = "localhost";
        int port = 9000;
        // 连接 Socket
        boolean isRunning = true;
        long attempt = 0;
        long maxNumRetries = 3;
        int delayBetweenRetries = 10000;
        while (isRunning) {
            try (Socket socket = new Socket()) {
                socket.connect(new InetSocketAddress(hostname, port), 0);
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {
                    char[] buffer = new char[10];
                    int bytes;
                    // Ctrl + D 模拟 EOF
                    while ((bytes = reader.read(buffer)) != -1) {
                        StringBuilder result = new StringBuilder();
                        result.append(buffer, 0, bytes);
                        System.out.println("[INFO] " + result);
                    }
                }
            }
            // 添加重试能力 最多重试3次 每次间隔10s
            if (isRunning) {
                attempt++;
                if (maxNumRetries == -1 || attempt < maxNumRetries) {
                    System.out.println("[INFO] retry ........");
                    Thread.sleep(delayBetweenRetries);
                } else {
                    break;
                }
            }
        }
    }
}
