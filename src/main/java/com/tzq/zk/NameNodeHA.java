package com.tzq.zk;

import org.apache.zookeeper.*;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs.Ids;

import java.util.List;

/**
 * 需求：
 * 模拟实现HA
 * <p>
 * 具体的功能：
 * 1、当开始启动namenode的时候，所有刚启动的namenode都需要去争抢成为active的namenode
 * 没有争抢成功的则成为standby的状态
 * 2、当active的namenode死掉之后，需要剩下的所有的stanby都需要去争抢成为active的状态
 * <p>
 * 在这个模拟实现中，假定所有的namenode之间的数据状态都是同步的。没有数据状态差别
 * <p>
 * 分析实现思路：
 * 见代码注释
 */
public class NameNodeHA {

    // 连接信息
    private static final String CONNECT_STR = "localhost:2181";

    // 会话超时时长   会话建立成功最长的等待时间
    private static final int TIME_OUT = 5000;

    // 存储active namenode的父级znode节点
    private static final String ACTIVE_PARENT = "/namenode_active";
    // 存储standby namenode的父级znode节点
    private static final String STANBY_PARENT = "/namenode_standbys";
    // 锁节点
    private static final String LOCK_ZNODE = "/namenode_lock";

    // 当前上线的节点名称
    private static final String NAMENODE_HOST = "node-01";

    static ZooKeeper zookeeper = null;

    public static void main(String[] args) throws Exception {

        /**
         * 获取连接
         *
         * 关于监听器的知识：
         * 有两种添加监听的方式：
         * 1、通过会话对象添加，这个会话对象中的所有的相应都能接收到，都在这个监听器对象中的process
         * 方法中执行业务逻辑的回调
         * 	在获取会话的时候添加的监听是属于全局监听，当前这个会话中的任何事件响应，都会回调这个监听器对象中的
         * process方法
         *
         * 2、在对应的三种添加监听的方式中，注入自定义的监听对象，那么注入的监听器对象是谁，
         * 当事件响应的时候，就回调这个监听器对象中的process方法
         */
        zookeeper = new ZooKeeper(CONNECT_STR, TIME_OUT, new Watcher() {

            @Override
            public void process(WatchedEvent event) {
                // 哪个znode节点
                String path = event.getPath();
                // 事件的类型
                EventType type = event.getType();

                // 如果是 ACTIVE_PARENT 的  NodeChildrenChanged 事件
                // 当active namenode死掉或者增加都会触发process回调
                if (path.equals(ACTIVE_PARENT) && type == EventType.NodeChildrenChanged) {

                    try {

                        // 争抢成为active namenode
                        List<String> onlyAtiveNM = zookeeper.getChildren(ACTIVE_PARENT, null);
                        if (onlyAtiveNM.size() == 0) {
                            // 原来的active namenode死掉了
                            // 正式实现：争抢成为active namenode
                            // 抢锁: 使用创建一个znode来模式实现抢锁，谁创建成功就是谁获取到了这把锁

                            // 注册监听
                            // 关注 LOCK_ZNODE 的  NodeCreated 事件
                            zookeeper.exists(LOCK_ZNODE, true);

                            // 创建锁节点
                            // 触发了 LOCK_ZNODE 的  NodeCreated 事件
                            if (zookeeper.exists(LOCK_ZNODE, false) == null) {
                                zookeeper.create(LOCK_ZNODE, NAMENODE_HOST.getBytes(),
                                        Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                            }

                        } else if (onlyAtiveNM.size() == 1) {
                            // 相当于已经有一个anmenode把自己切换成为active了
                            // 所以不需要做什么操作
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                } else if (path.equals(LOCK_ZNODE) && type == EventType.NodeCreated) {
                    String namenode_lock_znode = null;
                    try {
                        // 真正的来判断，谁创建成功的锁节点，如果是自己创建成功的，则切换自己的状态成为 active
                        byte[] data = zookeeper.getData(LOCK_ZNODE, false, null);
                        // namenode_lock_znode
                        // 这个对象 namenode_lock_znode 就是谁创建成功的那个namenode的节点名称
                        namenode_lock_znode = new String(data, "UTF-8");
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                    // 需要判断，是否是自己创建成功的锁节点
                    if (NAMENODE_HOST.equals(namenode_lock_znode)) {
                        // 是自己创建成功的锁节点
                        // 切换自己的状态

                        try {
                            // 首先删除自己在  STANDBY_PERENT节点下的该表自己的znode
                            String deletePath = STANBY_PARENT + "/" + NAMENODE_HOST;
                            if (zookeeper.exists(deletePath, false) != null) {
                                zookeeper.delete(deletePath, -1);
                            }

                            // 再创建一个znode节点在ACTIVE_PARNET下面
                            String createPath = ACTIVE_PARENT + "/" + NAMENODE_HOST;
                            zookeeper.create(createPath, NAMENODE_HOST.getBytes(),
                                    Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

                            System.out.println(NAMENODE_HOST + " 注册成为active角色");

                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        } catch (KeeperException e) {
                            e.printStackTrace();
                        }
                    } else {

                        // 判断得出结果 锁节点不是自己创建成功，不要成为active
                        // 什么都不做
                    }
                }
            }
        });


        /**
         * 执行各种操作
         */

        // 确保两个父节点存在
        if (zookeeper.exists(ACTIVE_PARENT, null) == null) {
            zookeeper.create(ACTIVE_PARENT, "storage active namenode data".getBytes(),
                    Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
        if (zookeeper.exists(STANBY_PARENT, null) == null) {
            zookeeper.create(STANBY_PARENT, "storage standby namenode data".getBytes(),
                    Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }

        // 先来判断是否有active的namenode
        List<String> activeNM = zookeeper.getChildren(ACTIVE_PARENT, null);
        if (activeNM.size() == 1) {

            System.out.println(activeNM.get(0) + " 节点是active角色, 自己 " + NAMENODE_HOST + "成为standby角色");

            // 如果有active的namenode， 则自动成为 standby的namenode
            // 到 STANBY_PARENT 这个znode节点下，创建一个子节点代表当前这个standby namenode
            String standByPath = STANBY_PARENT + "/" + NAMENODE_HOST;
            zookeeper.create(standByPath, NAMENODE_HOST.getBytes(),
                    Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

            // 注册监听：关注现在的active namenode 是否死掉
            // ACTIVE_PARENT 的  NodeChildrenChanged 事件
            // 关心是否 active 的namenode 死掉
            zookeeper.getChildren(ACTIVE_PARENT, true);
        } else {

            // 当发现没有active的namenode的时候：
            // 先争抢锁
            // 争抢锁争抢到了的话，就切换自己的状态
            // 注册监听
            // 关注 LOCK_ZNODE 的  NodeCreated 事件
            zookeeper.exists(LOCK_ZNODE, true);

            // 创建锁节点
            // 触发了 LOCK_ZNODE 的  NodeCreated 事件
            zookeeper.create(LOCK_ZNODE, NAMENODE_HOST.getBytes(),
                    Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            System.out.println("发现没有active，去争抢成为" + NAMENODE_HOST + " 节点成为standby节点");
        }

        /**
         * 关闭连接
         */
        while (true) {
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

}
