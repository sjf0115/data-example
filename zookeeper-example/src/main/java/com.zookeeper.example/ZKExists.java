package com.zookeeper.example;

import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;

/**
 * 判断指定路径节点是否存在
 * @author jifeng.sjf
 * @date 2019/8/25 下午8:55
 */
public class ZKExists {
    private static ZooKeeper zk;
    private static ZooKeeperConnection conn;

    // 同步方式判断指定路径的节点是否存在
    public static Stat existSync(String path) throws KeeperException,InterruptedException {
        return zk.exists(path, true);
    }

    // 异步方式判断指定路径的节点是否存在
    public static void existAsync(String path) {
        zk.exists(path, true,
            new AsyncCallback.StatCallback() {
                public void processResult(int resultCode, String path, Object ctx, Stat stat) {
                    System.out.println("ResultCode:" + resultCode);
                    System.out.println("Path: " + path);
                    System.out.println("Ctx: " + ctx);
                    System.out.println("Stat: " + stat);
                }
            }, "exist_async");
    }

    public static void main(String[] args) throws InterruptedException, KeeperException, IOException {
        String path1 = "/demo/node1";
        String path2 = "/demo/node2";
        conn = new ZooKeeperConnection();
        zk = conn.connect("localhost");
        // 同步方式判断指定路径的节点是否存在
        Stat stat = existSync(path1);
        if(stat != null) {
            System.out.println("Node exists and the node version is " + stat.getVersion());
        } else {
            System.out.println("Node does not exists");
        }
        // 同步方式判断指定路径的节点是否存在
        existAsync(path2);
        // 断开连接
        zk.close();
    }
}
