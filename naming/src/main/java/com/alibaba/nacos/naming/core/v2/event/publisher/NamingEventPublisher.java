/*
 * Copyright 1999-2020 Alibaba Group Holding Ltd.
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

package com.alibaba.nacos.naming.core.v2.event.publisher;

import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.common.notify.Event;
import com.alibaba.nacos.common.notify.ShardedEventPublisher;
import com.alibaba.nacos.common.notify.listener.Subscriber;
import com.alibaba.nacos.common.utils.ThreadUtils;
import com.alibaba.nacos.naming.misc.Loggers;
import com.alipay.sofa.jraft.util.concurrent.ConcurrentHashSet;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

/**
 * Event publisher for naming event.
 * 事件发布者本质是一个线程
 * @author xiweng.yy
 */
public class NamingEventPublisher extends Thread implements ShardedEventPublisher {
    
    private static final String THREAD_NAME = "naming.publisher-";
    
    private static final int DEFAULT_WAIT_TIME = 60;

    /**
     * 订阅者列表
     * 一类事件 对应 一个订阅者集合
     */
    private final Map<Class<? extends Event>, Set<Subscriber<? extends Event>>> subscribes = new ConcurrentHashMap<>();
    
    private volatile boolean initialized = false;
    
    private volatile boolean shutdown = false;
    
    private int queueMaxSize = -1;

    /**
     * 阻塞队列，存放事件
     */
    private BlockingQueue<Event> queue;
    
    private String publisherName;

    /**
     * 在事件发布者初始化的时候，就被当做一个线程，然后放在了NotifyCenter的publishMap里面，
     * 并且直接调用了其start方法启动了它，然后其内部保存了一个阻塞队列，用来暂存事件，当监听到
     * 里面的阻塞队列来了事件之后，就会消费它
     */
    @Override
    public void init(Class<? extends Event> type, int bufferSize) {
        this.queueMaxSize = bufferSize;
        this.queue = new ArrayBlockingQueue<>(bufferSize);
        this.publisherName = type.getSimpleName();
        super.setName(THREAD_NAME + this.publisherName);
        // 设置为守护线程，后台执行
        super.setDaemon(true);
        // 启动线程
        super.start();
        initialized = true;
    }
    
    @Override
    public long currentEventSize() {
        return this.queue.size();
    }
    
    @Override
    public void addSubscriber(Subscriber subscriber) {
        addSubscriber(subscriber, subscriber.subscribeType());
    }
    
    @Override
    public void addSubscriber(Subscriber subscriber, Class<? extends Event> subscribeType) {
        subscribes.computeIfAbsent(subscribeType, inputType -> new ConcurrentHashSet<>());
        subscribes.get(subscribeType).add(subscriber);
    }
    
    @Override
    public void removeSubscriber(Subscriber subscriber) {
        removeSubscriber(subscriber, subscriber.subscribeType());
    }
    
    @Override
    public void removeSubscriber(Subscriber subscriber, Class<? extends Event> subscribeType) {
        subscribes.computeIfPresent(subscribeType, (inputType, subscribers) -> {
            subscribers.remove(subscriber);
            return subscribers.isEmpty() ? null : subscribers;
        });
    }
    
    @Override
    public boolean publish(Event event) {
        // 校验publisher是否初始化
        checkIsStart();
        // 将事件存入阻塞队列中，这样订阅者就可以从队列中取出任务执行
        boolean success = this.queue.offer(event);
        // 如果存入队列失败，则直接通知订阅者处理事件
        if (!success) {
            Loggers.EVT_LOG.warn("Unable to plug in due to interruption, synchronize sending time, event : {}", event);
            handleEvent(event);
        }
        return true;
    }
    
    @Override
    public void notifySubscriber(Subscriber subscriber, Event event) {
        if (Loggers.EVT_LOG.isDebugEnabled()) {
            Loggers.EVT_LOG.debug("[NotifyCenter] the {} will received by {}", event, subscriber);
        }
        // 执行订阅者的onEvent方法，即每个订阅者对于该事件的处理逻辑
        // 允许订阅者支持同步还是异步方式处理
        final Runnable job = () -> subscriber.onEvent(event);
        final Executor executor = subscriber.executor();
        if (executor != null) {
            // 线程池异步执行
            executor.execute(job);
        } else {
            try {
                // 同步执行
                job.run();
            } catch (Throwable e) {
                Loggers.EVT_LOG.error("Event callback exception: ", e);
            }
        }
    }
    
    @Override
    public void shutdown() throws NacosException {
        this.shutdown = true;
        this.queue.clear();
    }
    
    @Override
    public void run() {
        try {
            waitSubscriberForInit();
            handleEvents();
        } catch (Exception e) {
            Loggers.EVT_LOG.error("Naming Event Publisher {}, stop to handle event due to unexpected exception: ",
                    this.publisherName, e);
        }
    }
    
    private void waitSubscriberForInit() {
        // To ensure that messages are not lost, enable EventHandler when
        // waiting for the first Subscriber to register
        for (int waitTimes = DEFAULT_WAIT_TIME; waitTimes > 0; waitTimes--) {
            if (shutdown || !subscribes.isEmpty()) {
                break;
            }
            ThreadUtils.sleep(1000L);
        }
    }
    
    private void handleEvents() {
        while (!shutdown) {
            try {
                // 从阻塞队列中取出事件
                final Event event = queue.take();
                handleEvent(event);
            } catch (InterruptedException e) {
                Loggers.EVT_LOG.warn("Naming Event Publisher {} take event from queue failed:", this.publisherName, e);
                // set the interrupted flag
                Thread.currentThread().interrupt();
            }
        }
    }
    
    private void handleEvent(Event event) {
        // 获取事件类型
        Class<? extends Event> eventType = event.getClass();
        // 获取到订阅这个事件的所有订阅者
        Set<Subscriber<? extends Event>> subscribers = subscribes.get(eventType);
        if (null == subscribers) {
            if (Loggers.EVT_LOG.isDebugEnabled()) {
                Loggers.EVT_LOG.debug("[NotifyCenter] No subscribers for slow event {}", eventType.getName());
            }
            return;
        }
        for (Subscriber subscriber : subscribers) {
            // 挨个通知每个事件订阅者
            notifySubscriber(subscriber, event);
        }
    }
    
    void checkIsStart() {
        if (!initialized) {
            throw new IllegalStateException("Publisher does not start");
        }
    }
    
    public String getStatus() {
        return String.format("Publisher %-30s: shutdown=%5s, queue=%7d/%-7d", publisherName, shutdown,
                currentEventSize(), queueMaxSize);
    }
}
