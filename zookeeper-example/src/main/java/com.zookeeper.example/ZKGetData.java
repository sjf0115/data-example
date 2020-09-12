package com.zookeeper.example;

import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;

/**
 * 获取指定节点中的数据
 * @author jifeng.sjf
 * @date 2019/8/27 上午9:50
 */
public class ZKGetData {

    private static ZooKeeper zk;
    private static ZooKeeperConnection conn;

    // 同步方式获取数据
    public static void getDataSync(String path) throws KeeperException, InterruptedException {
        Stat stat = new Stat();
        byte[] data = zk.getData(path, true, stat);
        System.out.println("Data: " + new String(data));
        System.out.println("Stat: " + stat);
    }

    // 异步方式获取数据
    public static void getDataAsync(String path){
        zk.getData(path, true, new AsyncCallback.DataCallback(){
            public void processResult(int resultCode, String path, Object ctx, byte[] data, Stat stat) {
                System.out.println("ResultCode: " + resultCode);
                System.out.println("Path: " + path);
                System.out.println("Ctx: " + ctx);
                System.out.println("Data: " + new String(data));
                System.out.println("Stat: " + stat);
            }
        }, "get_data_async");
    }

    public static void main(String[] args) throws InterruptedException, KeeperException, IOException {
        String path1 = "/test/node1";
        String path2 = "/test/node2";
        // 连接服务器
        conn = new ZooKeeperConnection();
        zk = conn.connect("localhost");
        // 同步方式获取数据
        getDataSync(path1);
        // 异步方式获取数据
        getDataAsync(path2);
        // 断开连接
        conn.close();
    }
}
