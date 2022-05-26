package com.flink.connector.socket;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.types.Row;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Socket;

/**
 * 功能：Socket SourceFunction
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/5/26 下午11:01
 */
public class SocketSourceFunction extends RichSourceFunction<Row> implements ResultTypeQueryable<Row> {

    private final String hostname;
    private final int port;
    private final byte byteDelimiter;
    private final long maxNumRetries;
    private final long delayBetweenRetries;

    //private final DeserializationSchema<Row> deserializer;

    private volatile boolean isRunning = true;
    private Socket currentSocket;

    public SocketSourceFunction(String hostname, int port, byte byteDelimiter, long maxNumRetries, long delayBetweenRetries) {
        this.hostname = hostname;
        this.port = port;
        this.byteDelimiter = byteDelimiter;
        this.maxNumRetries = maxNumRetries;
        this.delayBetweenRetries = delayBetweenRetries;
    }

    public SocketSourceFunction(String hostname, int port, byte byteDelimiter) {
        this.hostname = hostname;
        this.port = port;
        this.byteDelimiter = byteDelimiter;
        this.maxNumRetries = 0;
        this.delayBetweenRetries = 500;
    }

    @Override
    public TypeInformation<Row> getProducedType() {
        //return deserializer.getProducedType();
        return null;
    }

    @Override
    public void run(SourceContext<Row> sourceContext) throws Exception {
        long attempt = 0;
        while (isRunning) {
            try (final Socket socket = new Socket()) {
                currentSocket = socket;
                socket.connect(new InetSocketAddress(hostname, port), 0);
                try (InputStream stream = socket.getInputStream()) {
                    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
                    int b;
                    while ((b = stream.read()) >= 0) {
                        if (b != byteDelimiter) {
                            buffer.write(b);
                        }
                        else {
                            //sourceContext.collect(deserializer.deserialize(buffer.toByteArray()));
                            buffer.reset();
                        }
                    }
                }
            }

            if (isRunning) {
                attempt++;
                if (maxNumRetries == -1 || attempt < maxNumRetries) {
                    Thread.sleep(delayBetweenRetries);
                } else {
                    break;
                }
            }
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
        try {
            currentSocket.close();
        } catch (Throwable t) {
            // ignore
        }
    }
}
