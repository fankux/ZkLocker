/**
 * Created by fankux on 15-1-4.
 * 基于zookeeper的分布式锁
 */

package com.fankux.zklocker;


import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.SortedSet;
import java.util.concurrent.CountDownLatch;

public class ZkLocker {
    private static final Logger logger = LoggerFactory.getLogger(ZkLocker.class);

    private static final String DIR = "/parent";
    private static final String PREFIX = "dlk-";
    private static final List<ACL> ACLS = ZooDefs.Ids.OPEN_ACL_UNSAFE;
    private static final long RETRY_DELAY = 500l;
    private static final int RETRY_COUNT = 10;

    private ZooKeeper zookeeper;
    private ZkLockerListener listener;  /* 获得锁, 释放锁回调 */
    private ZkLockerNode idNode;        /* 当前会话节点 */
    private String id;                  /* 当前会话节点名 */
    private long tid = Thread.currentThread().getId();

    public ZkLocker(ZooKeeper zookeeper, ZkLockerListener listener) {
        this.zookeeper = zookeeper;
        this.listener = listener;
    }

    private class ZkLockWatcher implements Watcher {
        private CountDownLatch watcherLatch;

        public ZkLockWatcher(CountDownLatch watcherLatch) {
            this.watcherLatch = watcherLatch;
        }

        public void process(WatchedEvent event) {
            watcherLatch.countDown();
        }
    }

    /* 得到属于当前会话的节点 */
    private String getCurrentNode() throws KeeperException, InterruptedException {
//        String id = null;
        String curPrefix = PREFIX + zookeeper.getSessionId() + '-';
        /* 官方实现在此处先getChildren一次,
         * 是因为每一个会话具有不同sessionId, 按此前缀来的话, 如果找到则必然是本会话创建的
         * 出现这种情况在于会话断线重连, 但是没有会话过期, 如果是单会话多线程, 则在lock方法锁即可 */
//        List<String> names = zookeeper.getChildren(DIR, false);
//        for (String name : names) { /* 找到第一个即可 */
//            if (name.startsWith(curPrefix)) {
//                id = DIR + '/' + name;
//            }
//        }
//        if (id == null) {
        id = zookeeper.create(DIR + '/' + curPrefix, null, ACLS, CreateMode.EPHEMERAL_SEQUENTIAL);
        logger.debug("{}:{}-创建子节点", Thread.currentThread().getId(), id);
//        }
        return id;
    }

    private List<String> getChildList() throws KeeperException, InterruptedException {
        List<String> results = Lists.newArrayList();
        List<String> names = zookeeper.getChildren(DIR, false);
        for (String name : names) { /* 找到第一个即可 */
            if (name.startsWith(PREFIX)) {
                results.add(name);
            }
        }
        return results;
    }

    private boolean innerLock() throws KeeperException, InterruptedException {
        if (!ZkLockerFactory.isActive()) {
            return false;
        }

        boolean flag;
        do {
            flag = false;
            if (id == null) {
                id = getCurrentNode();
                logger.debug("{}:{}-获得当前节点", tid, id);
                idNode = new ZkLockerNode(id);
            }

            logger.debug("{}:{}-准备获得节点列表", tid, id);
            List<String> names = getChildList();
            logger.debug("{}:{}-成功获得节点列表:{}", tid, id, names);
            /* 上面getCurrentNode得到不是本身请求的节点, 然后其拥有者释放锁后节点被删除, 所以没有得到列表 */
            if (names.size() <= 0) {
                id = null;
                continue;
            }

            SortedSet<ZkLockerNode> sortedNames = Sets.newTreeSet();
            for (String name : names) {
                sortedNames.add(new ZkLockerNode(DIR + '/' + name));
            }

            String ownId = sortedNames.first().getName();
            SortedSet<ZkLockerNode> lessThan = sortedNames.headSet(idNode);
            if (lessThan.size() > 0) { /* 存在更小的节点 */
                ZkLockerNode lastNode = lessThan.last();
                logger.debug("{}:{}-监听前一个节点:{}", tid, id, lastNode.getName());
                CountDownLatch watcherLatch = new CountDownLatch(1);
                Stat stat = zookeeper.exists(lastNode.getName(), new ZkLockWatcher(watcherLatch));
                logger.debug("{}:{}-成功监听前一个节点:{}", tid, id, lastNode.getName());
                if (stat != null) {
                    logger.debug("{}:{}-阻塞等待前一个节点:{}", tid, id, lastNode.getName());
                    watcherLatch.await();
                    flag = true;
                } else { /* 可能由于前一个节点在调用exist前已经被解锁并删除 */
                    logger.debug("{}:{}-不能找到当前节点前一个节点", tid, id);
                }
            } else {
                if (ownId != null && id != null && id.equals(ownId)) {
                    logger.debug("{}:{}-ZK分布式锁:获取锁成功", tid, id);
                    if (listener != null) {
                        listener.lockAcquired();
                    }
                    return true;
                }
            }
        } while (id == null || flag);

        return false;
    }

    public synchronized boolean lock() {
        for (int i = 0; i < RETRY_COUNT; ++i) {
            try {
                return innerLock();
            } catch (KeeperException.SessionExpiredException e) {
                logger.warn("{}:{}-会话过期, 获取锁失败, 尝试第{}重连....", tid, id, i + 1, e);
                delay(i);
            } catch (KeeperException.ConnectionLossException e) { /* 断线会自动重连 */
                logger.warn("{}:{}-失去连接, 尝试第{}次重连....", tid, id, i + 1, e);
                delay(i);
            } catch (Exception e) {
                logger.warn("{}:{}-出现异常, 尝试第{}次重连....", tid, id, i + 1, e);
                delay(i);
            }
        }
        return false;
    }

    public synchronized void unlock() {
        logger.debug("{}:{}-准备unlock", tid, id);
        if (!ZkLockerFactory.isActive() || id == null) {
            return;
        }

        try {
            logger.debug("{}:{}-准备删除当期节点", tid, id);
            zookeeper.delete(id, -1);
            logger.debug("{}:{}-成功删除当期节点", tid, id);
            logger.debug("{}:{}-成功unlock", tid, id);
        } catch (InterruptedException e) {
            logger.warn("{}:{}-unlock被interrupted", tid, id, e);
            Thread.currentThread().interrupt();
        } catch (KeeperException.NoNodeException e) {
            logger.warn("{}:{}-unlock异常", tid, id, e);
        } catch (KeeperException e) {
            logger.warn("{}:{}-unlock异常", tid, id, e);
        } catch (Exception e) {
            logger.warn("{}:{}-unlock异常", tid, id, e);
        } finally {
            if (listener != null) {
                listener.lockReleased();
            }
            id = null;
        }
    }

    private void delay(int retryTime) {
        try {
            Thread.sleep(RETRY_DELAY * retryTime);
        } catch (InterruptedException e) {
            logger.error("zk分布式锁:延迟Sleep被中断", e);
        }
    }

    private class ZkLockerNode implements Comparable<ZkLockerNode> {
        private String name;
        private int seq;

        public ZkLockerNode(String name) {
            this.name = name;

            int idx = name.lastIndexOf('-');
            this.seq = Integer.parseInt(name.substring(idx + 1));
        }

        public String getName() {
            return name;
        }

        public int getSeq() {
            return seq;
        }

        @Override
        public int compareTo(ZkLockerNode o) {
            if (seq < o.getSeq()) {
                return -1;
            } else if (seq > o.getSeq()) {
                return 1;
            } else {
                return 0;
            }
        }

        @Override
        public String toString() {
            return name;
        }
    }
}