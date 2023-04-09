package com.tzq.zk;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.Collections;
import java.util.List;

/**
 * 该示例实现了一个简单的队列系统，一旦加入到队列中，就会被赋予一个永久存在的位置。
 * 如果请求被处理，则会自动从队列中删除。这里使用了ZooKeeper框架来实现队列，通过该框架我们可以很好地处理队列的恢复、重试和冲突检测等问题。
 */
public class ZookeeperQueue implements Watcher {

    private static final String QUEUE_NAME = "/queue";
    private static final String NODE_NAME = "node-";

    private ZooKeeper zooKeeper;
    private String myName;

    public ZookeeperQueue(String host, int port) throws Exception {
        this.myName = NODE_NAME + Long.toHexString(System.currentTimeMillis());
        this.zooKeeper = new ZooKeeper(host + ":" + port, 300000, this);
        if (zooKeeper.exists(QUEUE_NAME, false) == null) {
            zooKeeper.create(QUEUE_NAME, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
    }

    public void join() throws Exception {
        String myNode = zooKeeper.create(QUEUE_NAME + "/" + myName, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        while (true) {
            List<String> children = zooKeeper.getChildren(QUEUE_NAME, true);
            Collections.sort(children);
            int i = children.indexOf(myNode.substring(QUEUE_NAME.length() + 1));
            if (i == 0) {
                System.out.println("I am the leader.");
                return;
            } else {
                String watchedNode = QUEUE_NAME + "/" + children.get(i - 1);
                Stat stat = zooKeeper.exists(watchedNode, true);
                if (stat != null) {
                    synchronized (this) {
                        wait();
                    }
                }
            }
        }
    }

    @Override
    public void process(WatchedEvent event) {
        synchronized (this) {
            notifyAll();
        }
    }

    public static void main(String[] args) throws Exception {
        ZookeeperQueue queue = new ZookeeperQueue("127.0.0.1", 2181);
        queue.join();
    }
}