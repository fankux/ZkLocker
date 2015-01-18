/**
 * Created by fankux on 15-1-6.
 * zookeeper分布式锁工厂
 * 以静态的方式只维护一个zookeeper会话连接, 断线会重连, 只有主动close后,会话会短线
 */

package com.fankux.zklocker;

import org.apache.zookeeper.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

public class ZkLockerFactory {
    private static final Logger logger = LoggerFactory.getLogger(ZkLockerFactory.class);
    private static final String ZK_URLS = "127.0.0.1:2181";
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
                logger.debug("zk分布式锁:zookeeper连接成功, thread:{}, session:{}", Thread.currentThread().getName(),
                        Long.toHexString(zookeeper.getSessionId()));
                active.set(true); /* 此时才是真正的zk开启状态 */
                watchLatch.countDown();
            }
        }
    }

    private static ZooKeeper start() {
//        if (init.compareAndSet(false, true)) {
        try {
            logger.debug("zk分布式锁:准备初始化ZK");
            watchLatch = new CountDownLatch(1);
            zookeeper = new ZooKeeper(ZK_URLS, ZK_SESSION_TIMEOUT, watcher);
            watchLatch.await();
            logger.debug("zk分布式锁:完成初始化ZK:{}", zookeeper);
            //ensureParentDir();
            return zookeeper;
        } catch (Exception e) {
            logger.error("!!!!!!zk分布式锁:zookeeper连接失败!!!!!", e);
            throw new RuntimeException("zk分布式锁:zookeeper连接失败", e);
        }
//        }
    }

    public synchronized static ZooKeeper restart() {
        close();
        return start();
    }

    /**
     * 不要尝试在请求获得锁期间close(关闭会话), 这会导致别的请求也获得锁
     * 另外, 在active由true 设为false 与 zookeeper.close()之前的间隙,
     * 可能有别的线程重新初始化了zookeeper, 然后下面的代码又将其关闭了, 这样则状态混乱了.
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

    public synchronized static ZkLocker getLocker() {
        if (!isActive()) {
            start();
        }
        return new ZkLocker(zookeeper, null);
    }

    public synchronized static ZkLocker getLocker(ZkLockerListener listener) {
        if (!isActive()) {
            start();
        }
        return new ZkLocker(zookeeper, listener);
    }

    public static boolean isActive() {
        return active.get();
    }
}