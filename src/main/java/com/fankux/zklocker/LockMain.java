/**
 * Created by fankux on 15-1-5.
 * zookeeper分布式锁测试main
 */

package com.fankux.zklocker;

import com.google.common.base.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class LockMain {
    public static void main(String[] args) throws InterruptedException {
        final int threadCount = 8;
        ThreadPoolExecutor threadPool = (ThreadPoolExecutor) Executors.newFixedThreadPool(1000);
        threadPool.setRejectedExecutionHandler(new ThreadPoolExecutor.DiscardOldestPolicy());
        /* 测试方案, 起两个线程, 第一个线程执行中会阻塞很长时间, 所以若另一个线程后lock的话, 会加锁失败 */
        Stopwatch start = Stopwatch.createStarted();
        for (int i = 0; i < threadCount; ++i) {
            threadPool.execute(new ZkLockerTestRun(i));
        }
        threadPool.shutdown();
        threadPool.awaitTermination(1, TimeUnit.HOURS);
        System.out.println("thread count:" + threadCount + "; time:" + start.elapsed(TimeUnit.MILLISECONDS) + "ms");
    }
}

class ZkLockerTestRun implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(ZkLockerTestRun.class);
    private int seq = 0;

    public ZkLockerTestRun(int sequence) {
        seq = sequence;
    }

    /* 让第一个线程先获得锁, 然后阻塞一段时间 */
    private ZkLocker delayFirst() {
        if (seq == 1) {
            try {
                Thread.sleep(1000l); /* 让另一个线程后进锁, 应该返回false */
            } catch (InterruptedException e) {
                //do nothing
            }
            return ZkLockerFactory.getLocker(new LockerListenerDelay(1000));
        }
        return ZkLockerFactory.getLocker();
    }

    /* 让第1个线程先获得锁, 然后关闭zk会话, 等到过了zk 的 sessionTimeout 时间再让线程2尝试获得锁
     * 会产生unlock的sessionExpire, 分析如下:
     * 线程1首先会获得锁,然后立即关闭会话, 然后阻塞, 线程2首先阻塞再尝试获得锁, 两个阻塞的时间是一致的,
     * 就造成了两个线程同时去尝试unlock, 那么必然会有一个发生unlock的异常
     * 这种情形暴露了一个问题, 线程获得锁后主动的关闭会话,会导致别的请求也获得锁, 而此线程此时可能并没有主动unlock  */
    private ZkLocker quitFirstForSessionTimeOut() {
        if (seq == 0) { /* 第一个线程 */
            return ZkLockerFactory.getLocker(new ZkLockerListener() {
                @Override
                public void lockAcquired() {
                    ZkLockerFactory.close();
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }

                @Override
                public void lockReleased() {
                }
            });
        } else if (seq == 1) { /* 第二个线程 */
            try {
                Thread.sleep(5000); /* 让另一个线程后进锁, 应该返回false */
            } catch (InterruptedException e) {
                //do nothing
            }
            return ZkLockerFactory.getLocker();
        }
        return ZkLockerFactory.getLocker();
    }

    /* 竞争, 每个延迟2秒, 期望效果每2秒一个请求获得锁 */
    private ZkLocker delay() {
        return ZkLockerFactory.getLocker(new LockerListenerDelay(2000));
    }

    /* 竞争, 要测试多线程同时启动zk客户端, 只有一个成功 */
    private ZkLocker compete() {
        return ZkLockerFactory.getLocker();
    }

    /* 竞争, 抢到锁的立即关闭客户端, 会产生sessionExpire.
     * 问题分析:线程1(节点A)抢到锁, 然后线程1 unlock时delete了节点A, 并关闭了会话,
     * 然后线程2(节点B)监听节点A, 但是它并不知道节点A已经过期了, 所以sessionExpire, 发生在exist上, 然后通过重试可以再次获得 */
    private ZkLocker competeClose() {
        return ZkLockerFactory.getLocker(new ZkLockerListener() {
            @Override
            public void lockAcquired() {
                ZkLockerFactory.close();
            }

            @Override
            public void lockReleased() {

            }
        });
    }

    @Override
    public void run() {
        ZkLocker locker = competeClose();

        boolean result;
        if(seq%2 == 0){
            result = locker.lock("seqEven");
        }else{
            result = locker.lock("seqOdd");
        }
        logger.info("locker {} result : {}", seq, result);
        locker.unlock();
    }
}

class LockerListenerDelay implements ZkLockerListener {
    private long blockTime;

    public LockerListenerDelay(long blockTime) {
        this.blockTime = blockTime;
    }

    @Override
    public void lockAcquired() {
        try {
            Thread.sleep(blockTime);
        } catch (InterruptedException e) {
            //do nothing
        }
    }

    @Override
    public void lockReleased() {
    }
}