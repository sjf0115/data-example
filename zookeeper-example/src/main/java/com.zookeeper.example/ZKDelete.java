package com.zookeeper.example;

import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;

/**
 * 删除节点
 * @author jifeng.sjf
 * @date 2019/8/27 下午10:03
 */
public class ZKDelete {
    private static ZooKeeper zk;
    private static ZooKeeperConnection conn;

    // 同步方式删除节点
    public static void deleteSync(String path) throws KeeperException,InterruptedException {
        zk.delete(path, zk.exists(path,true).getVersion());
    }

    // 异步方式删除节点
    private static void deleteAsync(String path) throws KeeperException, InterruptedException {
        zk.delete(path, zk.exists(path,true).getVersion(), new AsyncCallback.VoidCallback() {
            public void processResult(int resultCode, String path, Object ctx) {
                System.out.println("ResultCode: " + resultCode);
                System.out.println("Path: " + path);
                System.out.println("Ctx: " + ctx);
            }
        }, "delete_async");
    }

    public static void main(String[] args) throws InterruptedException, KeeperException, IOException {
        String path1 = "/test/node1";
        String path2 = "/test/node2";
        // 连接服务器
        conn = new ZooKeeperConnection();
        zk = conn.connect("localhost");
        // 同步方式删除节点
        deleteSync(path1);
        // 异步方式删除节点
        deleteAsync(path2);
        // 断开连接
        conn.close();
    }
}
