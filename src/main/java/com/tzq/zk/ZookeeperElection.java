package com.tzq.zk;


import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
/**
 * @Title
 * @Author zhengqiang.tan
 * @Date 3/19/23 11:27 AM
 */
public class ZookeeperElection implements Watcher {

    private static final String HOST_PORT = "localhost:2181";
    private static final int SESSION_TIMEOUT = 5000;
    private static final String ELECTION_NODE = "/election";
    private static final String ELECTION_PREFIX = "/c_";

    private final String id;
    private final ZooKeeper zk;
    private String leaderId;

    private final CountDownLatch latch = new CountDownLatch(1);

    public ZookeeperElection(String id) throws IOException, InterruptedException, KeeperException {
        this.id = id;
        zk = new ZooKeeper(HOST_PORT, SESSION_TIMEOUT, this);
        latch.await();

        if (zk.exists(ELECTION_NODE, false) == null) { // 创建选举父节点
            zk.create(ELECTION_NODE, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
    }

    public void startElection() throws KeeperException, InterruptedException {
        String znode = ELECTION_NODE + ELECTION_PREFIX;

        String thisNode = zk.create(znode, id.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

        List<String> nodes = zk.getChildren(ELECTION_NODE, false);

        Collections.sort(nodes);

        // 判断自己是不是上一个节点的下一个节点
        int index = nodes.indexOf(thisNode.substring(ELECTION_NODE.length() + 1));
        if (index == 0) {
            becomeLeader();
        } else {
            String previousNode = ELECTION_NODE + "/" + nodes.get(index - 1);
            zk.getData(previousNode, false, new Stat()); // watch上一个节点，如果上一个节点失效，就重新竞选
        }
    }

    private void becomeLeader() throws KeeperException, InterruptedException {
        if (leaderId == null) {
            leaderId = id;
            System.out.println("I'm the leader: " + leaderId);
            // todo: do something as leader ,example: dispatcher task run ...
        }
    }

    @Override
    public void process(WatchedEvent event) {
        if (event.getState() == Event.KeeperState.SyncConnected) {
            if (event.getType() == Event.EventType.None) {
                latch.countDown();
            } else if (event.getType() == Event.EventType.NodeDeleted) { // 上一个节点失效了，重新开始选举
                try {
                    startElection();
                } catch (KeeperException | InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public void close() throws InterruptedException {
        zk.close();
    }

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        ZookeeperElection election1 = new ZookeeperElection("1");
        ZookeeperElection election2 = new ZookeeperElection("2");
        ZookeeperElection election3 = new ZookeeperElection("3");

        election1.startElection();
        election2.startElection();
        election3.startElection();

        Thread.sleep(Long.MAX_VALUE);

        election1.close();
        election2.close();
        election3.close();
    }
}