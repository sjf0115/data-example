package com.zookeeper.example;

import org.apache.zookeeper.*;

import java.io.IOException;

/**
 * 创建Znode节点
 * @author jifeng.sjf
 * @date 2019/8/25 下午7:29
 */
public class ZKCreate {
    // 创建ZooKeeper实例
    private static ZooKeeper zk;

    // 创建ZooKeeperConnection实例
    private static ZooKeeperConnection conn;

    // 同步方式创建节点
    public static void createNodeSync(String path, byte[] data) throws KeeperException,InterruptedException {
        String nodePath = zk.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        System.out.println("NodePath: " + nodePath);
    }

    // 异步方式创建节点
    public static void createNodeAsync(String path, byte[] data) {
        zk.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT,
                new AsyncCallback.StringCallback() {
                    public void processResult(int resultCode, String path, Object ctx, String name) {
                        System.out.println("ResultCode: " + resultCode);
                        System.out.println("Path: " + path);
                        System.out.println("Ctx: " + ctx);
                        System.out.println("Name: " + name);
                    }
                },
                "create_node_async"
        );
    }

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        String path1 = "/test/node1";
        String path2 = "/test/node2";
        byte[] data1 = "zookeeper node1".getBytes();
        byte[] data2 = "zookeeper node2".getBytes();
        // 建立连接
        conn = new ZooKeeperConnection();
        zk = conn.connect("localhost");
        // 同步方式创建节点
        createNodeSync(path1, data1);
        // 异步方式创建节点
        createNodeAsync(path2, data2);
        // 断开连接
        conn.close();
    }
}
