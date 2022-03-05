package com.flink.example.stream.source;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.Socket;

import static org.apache.flink.util.NetUtils.isValidClientPort;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * 功能：SocketSourceFunction
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/3/5 上午10:48
 */

public class SocketSourceFunction implements SourceFunction<String> {
    private static final Logger LOG = LoggerFactory.getLogger(SocketSourceFunction.class);
    // Socket
    private transient Socket currentSocket;
    private final String hostname;
    private final int port;
    // 将接收到的字符串拆分为记录的分隔符
    private final String delimiter;
    // 最大重试次数 -1 表示永远重试
    private final long maxNumRetries;
    // 重试间隔时间
    private final long delayBetweenRetries;
    private volatile boolean isRunning = true;

    // 默认连接最大重试次数
    private static final int CONNECTION_MAX_NUM_RETRIES = 0;
    // 默认连接重试间隔
    private static final int CONNECTION_RETRY_SLEEP = 500;
    // 默认连接超时时间
    private static final int CONNECTION_TIMEOUT_TIME = 0;

    public SocketSourceFunction(String hostname, int port, String delimiter) {
        this(hostname, port, delimiter, CONNECTION_MAX_NUM_RETRIES);
    }

    public SocketSourceFunction(String hostname, int port, String delimiter, long maxNumRetries) {
        this(hostname, port, delimiter, maxNumRetries, CONNECTION_RETRY_SLEEP);
    }

    public SocketSourceFunction(String hostname, int port, String delimiter, long maxNumRetries, long delayBetweenRetries) {
        checkArgument(isValidClientPort(port), "port is out of range");
        checkArgument(maxNumRetries >= -1, "maxNumRetries must be zero or larger (num retries), or -1 (infinite retries)");
        checkArgument(delayBetweenRetries >= 0, "delayBetweenRetries must be zero or positive");

        this.hostname = checkNotNull(hostname, "hostname must not be null");
        this.port = port;
        this.delimiter = delimiter;
        this.maxNumRetries = maxNumRetries;
        this.delayBetweenRetries = delayBetweenRetries;
    }

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        final StringBuilder buffer = new StringBuilder();
        long attempt = 0;

        while (isRunning) {
            // 从 Socket 获取记录并输出
            try (Socket socket = new Socket()) {
                currentSocket = socket;
                LOG.info("Connecting to server socket " + hostname + ':' + port);
                socket.connect(new InetSocketAddress(hostname, port), CONNECTION_TIMEOUT_TIME);
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {
                    char[] tmpBuffer = new char[8192];
                    int bytesRead;
                    // 循环读取
                    while (isRunning && (bytesRead = reader.read(tmpBuffer)) != -1) {
                        buffer.append(tmpBuffer, 0, bytesRead);
                        int delimiterPos;
                        // 循环分割字符串输出
                        while (buffer.length() >= delimiter.length() && (delimiterPos = buffer.indexOf(delimiter)) != -1) {
                            String record = buffer.substring(0, delimiterPos);
                            if (delimiter.equals("\n") && record.endsWith("\r")) {
                                record = record.substring(0, record.length() - 1);
                            }
                            LOG.info("[INFO] record: {}", record);
                            ctx.collect(record);
                            buffer.delete(0, delimiterPos + delimiter.length());
                        }
                    }
                }
            }

            // 重试连接
            if (isRunning) {
                attempt++;
                if (maxNumRetries == -1 || attempt < maxNumRetries) {
                    LOG.warn("Lost connection to server socket. Retrying in " + delayBetweenRetries + " msecs...");
                    Thread.sleep(delayBetweenRetries);
                } else {
                    break;
                }
            }
        }

        // 输出剩余
        if (buffer.length() > 0) {
            ctx.collect(buffer.toString());
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
        // 关闭Socket
        Socket theSocket = this.currentSocket;
        if (theSocket != null) {
            IOUtils.closeSocket(theSocket);
        }
    }
}
