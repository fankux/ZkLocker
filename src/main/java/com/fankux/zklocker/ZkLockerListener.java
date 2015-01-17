/**
 * Created by fankux on 15-1-5.
 * 定义了zklocker的两个回调
 */

package com.fankux.zklocker;


public interface ZkLockerListener {
    /**
     * call back called when the lock
     * is acquired
     */
    public void lockAcquired();

    /**
     * call back called when the lock is
     * released.
     */
    public void lockReleased();
}
