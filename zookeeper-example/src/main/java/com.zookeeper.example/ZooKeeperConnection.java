package com.zookeeper.example;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * 连接ZooKeeper服务器
 * @author jifeng.sjf
 * @date 2019/8/24 下午10:49
 */
public class ZooKeeperConnection {

    // ZooKeeper实例
    private ZooKeeper zoo;
    private final CountDownLatch connectedSignal = new CountDownLatch(1);

    /**
     * 连接服务器
     * @param host
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    public ZooKeeper connect(String host) throws IOException,InterruptedException {
        zoo = new ZooKeeper(host,5000, new Watcher() {
            public void process(WatchedEvent event) {
                Event.KeeperState state = event.getState();
                String path = event.getPath();
                // 连接状态
                if (state == Event.KeeperState.SyncConnected) {
                    System.out.println("客户端与ZooKeeper服务器处于连接状态");
                    connectedSignal.countDown();
                    if(event.getType() == Event.EventType.None && null == event.getPath()) {
                        System.out.println("监控状态变化");
                    }
                    else if(event.getType() == Event.EventType.NodeCreated) {
                        System.out.println("监控到节点[" + path + "]被创建");
                    }
                    else if(event.getType() == Event.EventType.NodeDataChanged) {
                        System.out.println("监控到节点[" + path + "]的数据内容发生变化");
                    }
                    else if(event.getType() == Event.EventType.NodeDeleted) {
                        System.out.println("监控到节点[" + path + "]被删除");
                    }
                }
                // 断开连接状态
                else if (state == Event.KeeperState.Disconnected){
                    System.out.println("客户端与ZooKeeper服务器处于断开连接状态");
                }
                // 会话超时
                else if (state == Event.KeeperState.Expired){
                    System.out.println("客户端与ZooKeeper服务器会话超时");
                }
            }
        });

        connectedSignal.await();
        return zoo;
    }

    /**
     * 断开与服务器的连接
     * @throws InterruptedException
     */
    public void close() throws InterruptedException {
        zoo.close();
    }


}
