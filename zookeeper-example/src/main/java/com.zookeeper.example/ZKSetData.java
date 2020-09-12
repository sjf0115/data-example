package com.zookeeper.example;

import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;

/**
 * 修改节点的数据内容
 * @author jifeng.sjf
 * @date 2019/8/27 下午7:54
 */
public class ZKSetData {
    private static ZooKeeper zk;
    private static ZooKeeperConnection conn;

    // 同步方式修改节点数据内容
    public static void setDataSync(String path, byte[] data) throws KeeperException,InterruptedException {
        zk.setData(path, data, zk.exists(path,false).getVersion());
    }

    // 异步方式修改节点数据内容
    public static void setDataAsync(String path, byte[] data) throws KeeperException, InterruptedException {
        zk.setData(path, data, zk.exists(path,false).getVersion(), new AsyncCallback.StatCallback() {
            public void processResult(int resultCode, String path, Object ctx, Stat stat) {
                System.out.println("ResultCode: " + resultCode);
                System.out.println("Path: " + path);
                System.out.println("Ctx: " + ctx);
                System.out.println("Stat: " + stat);
            }
        }, "set_data_async");
    }

    public static void main(String[] args) throws InterruptedException, KeeperException, IOException {
        String path1 = "/test/node1";
        String path2 = "/test/node2";

        byte[] data1 = "zookeeper node1 update success".getBytes();
        byte[] data2 = "zookeeper node2 update success".getBytes();
        // 连接服务器
        conn = new ZooKeeperConnection();
        zk = conn.connect("localhost");
        // 同步方式修改节点数据内容
        setDataSync(path1, data1);
        // 异步方式修改节点数据内容
        setDataAsync(path2, data2);
        // 断开连接
        conn.close();
    }
}
