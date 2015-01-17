/**
 * Created by fankux on 15-1-6.
 * zookeeper分布式锁工厂
 */

package com.fankux.zklocker;

import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

public class ZkLockerFactory {
    private static final Logger logger = LoggerFactory.getLogger(ZkLockerFactory.class);
    private static final String ZK_URLS = "127.0.0.1:2181";
    private static final String DIR = "/parent";
    private static final List<ACL> ACLS = ZooDefs.Ids.OPEN_ACL_UNSAFE;
    private static final int ZK_SESSION_TIMEOUT = 100;

    private static AtomicBoolean active = new AtomicBoolean(false); /* 开启状态 */
    private static CountDownLatch watchLatch;
    private static ZkWatcher watcher = new ZkWatcher();
    private static ZooKeeper zookeeper;

    private ZkLockerFactory() {
    }

    private static class ZkWatcher implements Watcher {
        @Override
        public void process(WatchedEvent event) {
            if (event.getType() == Event.EventType.NodeCreated) {
                logger.debug("zk分布式锁:节点创建:{}", event.getPath());
            } else if (event.getState() == Event.KeeperState.SyncConnected) {
                logger.debug("zk分布式锁:zookeeper连接成功, thread:{}", Thread.currentThread().getName());
                active.set(true); /* 此时才是真正的zk开启状态 */
                watchLatch.countDown();
            } else if (event.getState() == Event.KeeperState.Disconnected) {
                logger.warn("zk分布式锁:连接断线, 等待重连, 节点:{}", event.getPath());
            } else if (event.getState() == Event.KeeperState.Expired) {
                logger.warn("zk分布式锁:会话过期, 重连, 节点:{}", event.getPath());
                watchLatch.countDown();
                close();
                initZookeeper();
            }
            watchLatch.countDown();
        }
    }

    private static void initZookeeper() {
        if (isActive()) {
            return;
        }

        try {
            watchLatch = new CountDownLatch(1);
            zookeeper = new ZooKeeper(ZK_URLS, ZK_SESSION_TIMEOUT, watcher);
            watchLatch.await();
            ensureParentDir();
        } catch (Exception e) {
            logger.error("!!!!!!zk分布式锁:zookeeper连接失败!!!!!", e);
            throw new RuntimeException("zk分布式锁:zookeeper连接失败", e);
        }
    }

    /**
     * 不要尝试在请求获得锁期间close(关闭会话), 这会导致别的请求也获得锁
     */
    public static void close() {
        if (active.compareAndSet(true, false)) {
            try {
                zookeeper.close();
            } catch (InterruptedException e) {
                logger.warn("zk分布式锁:关闭zookeeper被interrupt", e);
            }
        }
    }

    public static boolean isActive() {
        return active.get();
    }

    public synchronized static ZkLocker getLocker() {
        if (!isActive()) {
            initZookeeper();
        }
        return new ZkLocker(zookeeper, null);
    }

    public synchronized static ZkLocker getLocker(ZkLockerListener listener) {
        if (!isActive()) {
            initZookeeper();
        }
        return new ZkLocker(zookeeper, listener);
    }

    /* 仅用于本地测试 */
    private static void ensureParentDir() {
        try {
            Stat stat = zookeeper.exists(DIR, false);
            if (stat != null) {
                return;
            }
            zookeeper.create(DIR, null, ACLS, CreateMode.PERSISTENT);
        } catch (Exception e) {
            logger.error("!!!zk分布式锁:父节点确认失败!!!");
            throw (RuntimeException) new RuntimeException(e.getMessage()).initCause(e);
        }
    }
}