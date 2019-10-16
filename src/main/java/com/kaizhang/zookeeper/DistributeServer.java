package com.kaizhang.zookeeper;

import java.io.IOException;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;

/**
 * @author kaizhang
 * 服务器（其实也是Zookeeper客户端）
 */
public class DistributeServer {

	public static void main(String[] args) throws Exception {
		
		DistributeServer server = new DistributeServer();
		
		// 1 连接zookeeper集群
		server.getConnect();
		
		// 2 注册节点
		server.register(args[0]);
		
		// 3 业务逻辑处理
		server.business();
	}

	private void business() throws InterruptedException {
	
		Thread.sleep(Long.MAX_VALUE);
	}

	private void register(String hostname) throws KeeperException, InterruptedException {
		
		String path = zkClient.create("/servers/server", hostname.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
		
		System.out.println(hostname +" is online ");
		
	}

	private static final String CONNECT_STRING = "192.168.17.101:2181,192.168.17.102:2181,192.168.17.103:2181";
	private static final int SESSION_TIMEOUT = 2000;
	private ZooKeeper zkClient;

	private void getConnect() throws IOException {
		
		zkClient = new ZooKeeper(CONNECT_STRING , SESSION_TIMEOUT , new Watcher() {
			
			@Override
			public void process(WatchedEvent event) {
				// TODO Auto-generated method stub
				
			}
		});
	}
}
