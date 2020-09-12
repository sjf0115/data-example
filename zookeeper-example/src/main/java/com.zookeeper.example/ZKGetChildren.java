package com.zookeeper.example;

import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.List;

/**
 * 获取节点的子节点列表
 * @author jifeng.sjf
 * @date 2019/8/27 下午9:48
 */
public class ZKGetChildren {
    private static ZooKeeper zk;
    private static ZooKeeperConnection conn;

    // 同步方式获取节点的子节点列表
    public static void getChildrenSync(String path) throws KeeperException, InterruptedException {
        List<String> children = zk.getChildren(path, true);
        for(int i = 0; i < children.size(); i++) {
            System.out.println("Child: " + children.get(i));
        }
    }

    // 异步方式获取节点的子节点列表
    public static void getChildrenAsync(String path) {
        zk.getChildren(path, true, new AsyncCallback.Children2Callback() {
            public void processResult(int resultCode, String path, Object ctx, List<String> children, Stat stat) {
                System.out.println("ResultCode: " + resultCode);
                System.out.println("Path: " + path);
                System.out.println("Ctx: " + ctx);
                System.out.println("Stat: " + stat);
                for(String child : children) {
                    System.out.println("Child: " + child);
                }
            }
        }, "get_children_async");
    }

    public static void main(String[] args) throws InterruptedException, KeeperException, IOException {
        String path = "/demo";
        // 连接服务器
        conn = new ZooKeeperConnection();
        zk = conn.connect("localhost");
        // 同步方式获取节点的子节点列表
        getChildrenSync(path);
        // 异步方式获取节点的子节点列表
        getChildrenAsync(path);
        // 断开连接
        conn.close();
    }
}
