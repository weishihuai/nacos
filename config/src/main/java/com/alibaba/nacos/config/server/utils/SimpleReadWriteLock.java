/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.nacos.config.server.utils;

/**
 * 最简单的读写锁实现。要求加锁和解锁必须成对调用。
 *
 * Simplest read-write lock implementation. Requires locking and unlocking must be called in pairs.
 *
 * @author Nacos
 */
public class SimpleReadWriteLock {
    
    /**
     * 零表示没有锁；负数表示写锁；正数表示读锁，数值代表读锁的数量。
     */
    private int status = 0;
    
    /**
     * 尝试加读锁，读锁是可以进行并行加锁的，就是读写锁互斥、写写互斥，但是读读不互斥
     */
    public synchronized boolean tryReadLock() {
        // 是否添加了写锁
        if (isWriteLocked()) {
            return false;
        } else {
            // 读锁数量+1
            status++;
            return true;
        }
    }
    
    /**
     * 释放读锁
     */
    public synchronized void releaseReadLock() {
        // when status equals 0, it should not decrement to negative numbers
        if (status == 0) {
            return;
        }
        status--;
    }
    
    /**
     * 尝试添加写锁
     * 写锁的时候 status = -1 尝试加写锁
     */
    public synchronized boolean tryWriteLock() {
        if (!isFree()) {
            return false;
        } else {
            status = -1;
            return true;
        }
    }

    /**
     * 释放写锁
     */
    public synchronized void releaseWriteLock() {
        status = 0;
    }

    /**
     * 是否是写锁
     */
    private boolean isWriteLocked() {
        return status < 0;
    }

    /**
     * 是否是没有锁
     */
    private boolean isFree() {
        return status == 0;
    }

}
