package com.spark.example.streaming.connector.receiver;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

/**
 * 功能：自定义 Socket Receiver
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2025/1/3 22:58
 */
public class CustomSocketReceiver extends Receiver<String> {
    private static final Logger LOG = LoggerFactory.getLogger(CustomSocketReceiver.class);
    private String host;
    private int port;
    private Socket socket;
    private BufferedReader reader;

    public CustomSocketReceiver(String host, int port) {
        super(StorageLevel.MEMORY_AND_DISK_2());
        this.host = host;
        this.port = port;
    }

    public CustomSocketReceiver() {
        this("localhost", 9100);
    }

    @Override
    public void onStart() {
        // 启动一个新的线程来接收数据
        /*new Thread(new Runnable() {
            @Override
            public void run() {
                receive();
            }
        }).start();*/
        new Thread(this::receive).start();
        LOG.info("onStart 启动一个新的线程来接收数据");
    }

    @Override
    public void onStop() {
        // 释放资源：关闭 socket 连接
        try {
            if (reader != null) {
                reader.close();
            }
            if (socket != null && !socket.isClosed()) {
                socket.close();
            }
        } catch (IOException e) {
            LOG.error("onStop 关闭 socket 连接失败", e);
        }
    }

    // 接收数据
    private void receive() {
        String line;
        try {
            socket = new Socket(host, port);
            reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
            // 在接收数据的线程中，需定期检查 isStopped() 方法，以便及时响应 Receiver 的停止
            while (!isStopped() && (line = reader.readLine()) != null) {
                LOG.info("[INFO] 接收数据：" + line);
                store(line);
            }
            // 如果断开连接 重启 Receiver
            if (!isStopped()) {
                restart("尝试再次重启 Receiver");
            }
        } catch(IOException e) {
            LOG.error("接收数据失败", e);
            if (!isStopped()) {
                restart("发生错误 尝试再次重启 Receiver");
            }
        }
    }
}
