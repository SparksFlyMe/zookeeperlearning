package com.kaizhang.zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

/**
 * @author kaizhang
 * zookeeper测试类
 */
public class ZookeeperTest {

    private static final String CONNECT_STRING = "192.168.17.101:2181,192.168.17.102:2181,192.168.17.103:2181";
    private static final int SESSION_TIMEOUT = 20000000;
    private ZooKeeper zkClient;

    @Before
    public void init() throws IOException {

        zkClient = new ZooKeeper(CONNECT_STRING, SESSION_TIMEOUT, new Watcher() {

            @Override
            public void process(WatchedEvent event) {

                System.out.println("---------start----------");
                List<String> children;
                try {
                    children = zkClient.getChildren("/", true);

                    for (String child : children) {
                        System.out.println(child);
                    }
                    System.out.println("---------end----------");
                } catch (KeeperException | InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        });
    }

    /**
     * 1 创建节点
     */
    @Test
    public void createNode() throws KeeperException, InterruptedException {

        String path = zkClient.create("/atguigu", "dahaigezuishuai".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        System.out.println(path);

    }

    /**
     * 2 获取子节点 并监控节点的变化
     */
    @Test
    public void getDataAndWatch() throws KeeperException, InterruptedException {

        List<String> children = zkClient.getChildren("/", true);

        for (String child : children) {
            System.out.println(child);
        }

        Thread.sleep(Long.MAX_VALUE);
    }

    /**
     * 3 判断节点是否存在
     */
    @Test
    public void exist() throws KeeperException, InterruptedException {

        Stat stat = zkClient.exists("/zookeeper", false);

        System.out.println(stat == null ? "not exist" : "exist");
    }
}
