package com.common.example.socket;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.Socket;

/**
 * 功能：Socket 简单示例
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/5/28 上午9:51
 */
public class SocketSimpleExample {
    public static void main(String[] args) throws IOException {

        String hostname = "localhost";
        int port = 9000;
        String delimiter = "\n";
        final StringBuilder result = new StringBuilder();
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(hostname, port), 0);
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {
                char[] buffer = new char[10];
                int bytes;
                while ((bytes = reader.read(buffer)) != -1) {
                    result.append(buffer, 0, bytes);

                    System.out.println(result);

                    int delimiterPos;
                    while (result.length() >= delimiter.length() && (delimiterPos = result.indexOf(delimiter)) != -1) {

                        String record = result.substring(0, delimiterPos);

                        if (delimiter.equals("\n") && record.endsWith("\r")) {
                            record = record.substring(0, record.length() - 1);
                        }
                        result.delete(0, delimiterPos + delimiter.length());
                    }
                }
            }
        }
    }
}
