package com.kaizhang.zookeeper;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author kaizhang
 * 客户端（作为监控）
 */
public class DistributeClient {

    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {

        DistributeClient client = new DistributeClient();

        // 1 获取zookeeper集群连接
        client.getConnect();

        // 2 注册监听
        client.getChildren();

        // 3 业务逻辑处理
        client.business();

    }

    private void business() throws InterruptedException {
        Thread.sleep(Long.MAX_VALUE);
    }

    private void getChildren() throws KeeperException, InterruptedException {

        List<String> children = zkClient.getChildren("/servers", true);

        // 存储服务器节点主机名称集合
        ArrayList<String> hosts = new ArrayList<>();

        for (String child : children) {

            byte[] data = zkClient.getData("/servers/" + child, false, null);

            hosts.add(new String(data));
        }

        // 将所有在线主机名称打印到控制台
        System.out.println(hosts);

    }

    private static final String CONNECT_STRING = "192.168.17.101:2181,192.168.17.102:2181,192.168.17.103:2181";
    private static final int SESSION_TIMEOUT = 20000;
    private ZooKeeper zkClient;

    private void getConnect() throws IOException {

        zkClient = new ZooKeeper(CONNECT_STRING, SESSION_TIMEOUT, new Watcher() {

            @Override
            public void process(WatchedEvent event) {

                try {
                    getChildren();
                } catch (KeeperException | InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

    }
}
