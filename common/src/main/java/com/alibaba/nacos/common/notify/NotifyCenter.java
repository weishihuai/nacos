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

package com.alibaba.nacos.common.notify;

import com.alibaba.nacos.api.exception.runtime.NacosRuntimeException;
import com.alibaba.nacos.common.JustForTest;
import com.alibaba.nacos.common.notify.listener.SmartSubscriber;
import com.alibaba.nacos.common.notify.listener.Subscriber;
import com.alibaba.nacos.common.spi.NacosServiceLoader;
import com.alibaba.nacos.common.utils.ClassUtils;
import com.alibaba.nacos.common.utils.MapUtil;
import com.alibaba.nacos.common.utils.ThreadUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.alibaba.nacos.api.exception.NacosException.SERVER_ERROR;

/**
 * 统一事件通知中心
 * Unified Event Notify Center.
 *
 * @author <a href="mailto:liaochuntao@live.com">liaochuntao</a>
 * @author zongtanghu
 */
public class NotifyCenter {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(NotifyCenter.class);

    /**
     * ringBufferSize/shareBufferSize这两个变量因为在静态块中初始化，然后并没有改变其初始值，所以不需要特殊处理
     */
    public static int ringBufferSize;
    
    public static int shareBufferSize;
    
    private static final AtomicBoolean CLOSED = new AtomicBoolean(false);

    /**
     * EventPublisher工厂
     */
    private static final EventPublisherFactory DEFAULT_PUBLISHER_FACTORY;

    /**
     * 单例，整个进程共享这个实例，所有的内部变量归所有线程共享，需要考虑并发问题
     */
    private static final NotifyCenter INSTANCE = new NotifyCenter();

    /**
     * 共享EventPublisher工厂
     */
    private DefaultSharePublisher sharePublisher;
    
    private static Class<? extends EventPublisher> clazz;
    
    /**
     * 事件发布器管理容器
     */
    private final Map<String, EventPublisher> publisherMap = new ConcurrentHashMap<>(16);

    // 静态代码块进行一些初始化工作。当NotifyCenter类被加载时，静态代码块会被执行，且只会执行一次。
    static {
        // Internal ArrayBlockingQueue buffer size. For applications with high write throughput,
        // this value needs to be increased appropriately. default value is 16384

        // 默认的生产者的阻塞队列大小
        String ringBufferSizeProperty = "nacos.core.notify.ring-buffer-size";
        // Integer.getInteger获取系统属性的值 System.getProperty("xxx")
        ringBufferSize = Integer.getInteger(ringBufferSizeProperty, 16384);

        // 共享生产者阻塞队列大小
        // The size of the public publisher's message staging queue buffer
        String shareBufferSizeProperty = "nacos.core.notify.share-buffer-size";
        // Integer.getInteger获取系统属性的值 System.getProperty("xxx")
        shareBufferSize = Integer.getInteger(shareBufferSizeProperty, 1024);

        // 这里采用了SPI的扩展机制，里面包含了JDK原生的SPI扩展机制，不过Nacos对这部分的内容加了缓存
        final Collection<EventPublisher> publishers = NacosServiceLoader.load(EventPublisher.class);
        Iterator<EventPublisher> iterator = publishers.iterator();

        if (iterator.hasNext()) {
            clazz = iterator.next().getClass();
        } else {
            // 没有配置，就用默认的DefaultPublisher生产者
            clazz = DefaultPublisher.class;
        }

        // 创建一个EventPublisher的工厂，主要是对EventPublisher进行创建并初始化
        // 这里是一个Lambda表达式，只是声明了一个回调函数，并不会马上执行内部逻辑
        DEFAULT_PUBLISHER_FACTORY = (cls, buffer) -> {
            try {
                EventPublisher publisher = clazz.newInstance();
                // 实际上是对于每个publisher都启动了一个线程
                publisher.init(cls, buffer);
                return publisher;
            } catch (Throwable ex) {
                LOGGER.error("Service class newInstance has error : ", ex);
                throw new NacosRuntimeException(SERVER_ERROR, ex);
            }
        };
        
        try {

            // 创建并初始化共享的生产者
            // Create and init DefaultSharePublisher instance.
            INSTANCE.sharePublisher = new DefaultSharePublisher();
            INSTANCE.sharePublisher.init(SlowEvent.class, shareBufferSize);
            
        } catch (Throwable ex) {
            LOGGER.error("Service class newInstance has error : ", ex);
        }

        // JVM关闭的回调钩子
        ThreadUtils.addShutdownHook(NotifyCenter::shutdown);
    }
    
    @JustForTest
    public static Map<String, EventPublisher> getPublisherMap() {
        return INSTANCE.publisherMap;
    }
    
    @JustForTest
    public static EventPublisher getPublisher(Class<? extends Event> topic) {
        if (ClassUtils.isAssignableFrom(SlowEvent.class, topic)) {
            return INSTANCE.sharePublisher;
        }
        return INSTANCE.publisherMap.get(topic.getCanonicalName());
    }
    
    @JustForTest
    public static EventPublisher getSharePublisher() {
        return INSTANCE.sharePublisher;
    }
    
    /**
     * Shutdown the several publisher instance which notify center has.
     */
    public static void shutdown() {
        if (!CLOSED.compareAndSet(false, true)) {
            return;
        }
        LOGGER.warn("[NotifyCenter] Start destroying Publisher");
        
        for (Map.Entry<String, EventPublisher> entry : INSTANCE.publisherMap.entrySet()) {
            try {
                EventPublisher eventPublisher = entry.getValue();
                eventPublisher.shutdown();
            } catch (Throwable e) {
                LOGGER.error("[EventPublisher] shutdown has error : ", e);
            }
        }
        
        try {
            INSTANCE.sharePublisher.shutdown();
        } catch (Throwable e) {
            LOGGER.error("[SharePublisher] shutdown has error : ", e);
        }
        
        LOGGER.warn("[NotifyCenter] Destruction of the end");
    }
    
    /**
     * Register a Subscriber. If the Publisher concerned by the Subscriber does not exist, then PublihserMap will
     * preempt a placeholder Publisher with default EventPublisherFactory first.
     *
     * @param consumer subscriber
     */
    public static void registerSubscriber(final Subscriber consumer) {
        // 往工厂中注册订阅者
        registerSubscriber(consumer, DEFAULT_PUBLISHER_FACTORY);
    }
    
    /**
     * Register a Subscriber. If the Publisher concerned by the Subscriber does not exist, then PublihserMap will
     * preempt a placeholder Publisher with specified EventPublisherFactory first.
     *
     * @param consumer subscriber
     * @param factory  publisher factory.
     */
    public static void registerSubscriber(final Subscriber consumer, final EventPublisherFactory factory) {
        // If you want to listen to multiple events, you do it separately,
        // based on subclass's subscribeTypes method return list, it can register to publisher.
        if (consumer instanceof SmartSubscriber) {
            for (Class<? extends Event> subscribeType : ((SmartSubscriber) consumer).subscribeTypes()) {
                // For case, producer: defaultSharePublisher -> consumer: smartSubscriber.
                if (ClassUtils.isAssignableFrom(SlowEvent.class, subscribeType)) {
                    INSTANCE.sharePublisher.addSubscriber(consumer, subscribeType);
                } else {
                    // For case, producer: defaultPublisher -> consumer: subscriber.
                    addSubscriber(consumer, subscribeType, factory);
                }
            }
            return;
        }
        
        final Class<? extends Event> subscribeType = consumer.subscribeType();
        if (ClassUtils.isAssignableFrom(SlowEvent.class, subscribeType)) {
            INSTANCE.sharePublisher.addSubscriber(consumer, subscribeType);
            return;
        }
        // 添加订阅者
        addSubscriber(consumer, subscribeType, factory);
    }
    
    /**
     * 往生产中添加一个订阅者
     * Add a subscriber to publisher.
     *
     * @param consumer      subscriber instance.
     * @param subscribeType subscribeType.
     * @param factory       publisher factory.
     */
    private static void addSubscriber(final Subscriber consumer, Class<? extends Event> subscribeType,
            EventPublisherFactory factory) {
        
        final String topic = ClassUtils.getCanonicalName(subscribeType);
        synchronized (NotifyCenter.class) {
            // MapUtils.computeIfAbsent is a unsafe method.
            MapUtil.computeIfAbsent(INSTANCE.publisherMap, topic, factory, subscribeType, ringBufferSize);
        }
        EventPublisher publisher = INSTANCE.publisherMap.get(topic);
        // 将订阅者放入EventPublisher中
        if (publisher instanceof ShardedEventPublisher) {
            // 共享事件发布器
            ((ShardedEventPublisher) publisher).addSubscriber(consumer, subscribeType);
        } else {
            // DefaultPublisher
            publisher.addSubscriber(consumer);
        }
    }
    
    /**
     * Deregister subscriber.
     *
     * @param consumer subscriber instance.
     */
    public static void deregisterSubscriber(final Subscriber consumer) {
        if (consumer instanceof SmartSubscriber) {
            for (Class<? extends Event> subscribeType : ((SmartSubscriber) consumer).subscribeTypes()) {
                if (ClassUtils.isAssignableFrom(SlowEvent.class, subscribeType)) {
                    INSTANCE.sharePublisher.removeSubscriber(consumer, subscribeType);
                } else {
                    removeSubscriber(consumer, subscribeType);
                }
            }
            return;
        }
        
        final Class<? extends Event> subscribeType = consumer.subscribeType();
        if (ClassUtils.isAssignableFrom(SlowEvent.class, subscribeType)) {
            INSTANCE.sharePublisher.removeSubscriber(consumer, subscribeType);
            return;
        }
        
        if (removeSubscriber(consumer, subscribeType)) {
            return;
        }
        throw new NoSuchElementException("The subscriber has no event publisher");
    }
    
    /**
     * Remove subscriber.
     *
     * @param consumer      subscriber instance.
     * @param subscribeType subscribeType.
     * @return whether remove subscriber successfully or not.
     */
    private static boolean removeSubscriber(final Subscriber consumer, Class<? extends Event> subscribeType) {
        
        final String topic = ClassUtils.getCanonicalName(subscribeType);
        EventPublisher eventPublisher = INSTANCE.publisherMap.get(topic);
        if (null == eventPublisher) {
            return false;
        }
        if (eventPublisher instanceof ShardedEventPublisher) {
            ((ShardedEventPublisher) eventPublisher).removeSubscriber(consumer, subscribeType);
        } else {
            eventPublisher.removeSubscriber(consumer);
        }
        return true;
    }
    
    /**
     * Request publisher publish event Publishers load lazily, calling publisher. Start () only when the event is
     * actually published.
     *
     * @param event class Instances of the event.
     */
    public static boolean publishEvent(final Event event) {
        try {
            // event.getClass(): com.alibaba.nacos.naming.core.v2.event.client.ClientOperationEvent$ClientRegisterServiceEvent
            return publishEvent(event.getClass(), event);
        } catch (Throwable ex) {
            LOGGER.error("There was an exception to the message publishing : ", ex);
            return false;
        }
    }
    
    /**
     * Request publisher publish event Publishers load lazily, calling publisher.
     *
     * @param eventType class Instances type of the event type.
     * @param event     event instance.
     */
    private static boolean publishEvent(final Class<? extends Event> eventType, final Event event) {
        if (ClassUtils.isAssignableFrom(SlowEvent.class, eventType)) {
            return INSTANCE.sharePublisher.publish(event);
        }

        // topic = com.alibaba.nacos.naming.core.v2.event.client.ClientOperationEvent.ClientRegisterServiceEvent
        final String topic = ClassUtils.getCanonicalName(eventType);

        // 获取对应的EventPublisher进行发布，也就是通知订阅者去处理
        // Map<String, EventPublisher> publisherMap = new ConcurrentHashMap<>(16)
        // INSTANCE.publisherMap就是事件发布器管理容器
        EventPublisher publisher = INSTANCE.publisherMap.get(topic);
        if (publisher != null) {
            // 调用publish()发布事件
            return publisher.publish(event);
        }
        if (event.isPluginEvent()) {
            return true;
        }
        LOGGER.warn("There are no [{}] publishers for this event, please register", topic);
        return false;
    }
    
    /**
     * Register to share-publisher.
     *
     * @param eventType class Instances type of the event type.
     * @return share publisher instance.
     */
    public static EventPublisher registerToSharePublisher(final Class<? extends SlowEvent> eventType) {
        return INSTANCE.sharePublisher;
    }
    
    /**
     * Register publisher with default factory.
     *
     * @param eventType    class Instances type of the event type.
     * @param queueMaxSize the publisher's queue max size.
     */
    public static EventPublisher registerToPublisher(final Class<? extends Event> eventType, final int queueMaxSize) {
        return registerToPublisher(eventType, DEFAULT_PUBLISHER_FACTORY, queueMaxSize);
    }
    
    /**
     * Register publisher with specified factory.
     *
     * @param eventType    class Instances type of the event type.
     * @param factory      publisher factory.
     * @param queueMaxSize the publisher's queue max size.
     */
    public static EventPublisher registerToPublisher(final Class<? extends Event> eventType,
            final EventPublisherFactory factory, final int queueMaxSize) {
        if (ClassUtils.isAssignableFrom(SlowEvent.class, eventType)) {
            return INSTANCE.sharePublisher;
        }
        
        final String topic = ClassUtils.getCanonicalName(eventType);
        synchronized (NotifyCenter.class) {
            // MapUtils.computeIfAbsent is a unsafe method.
            MapUtil.computeIfAbsent(INSTANCE.publisherMap, topic, factory, eventType, queueMaxSize);
        }
        return INSTANCE.publisherMap.get(topic);
    }
    
    /**
     * Register publisher.
     *
     * @param eventType class Instances type of the event type.
     * @param publisher the specified event publisher
     */
    public static void registerToPublisher(final Class<? extends Event> eventType, final EventPublisher publisher) {
        if (null == publisher) {
            return;
        }
        final String topic = ClassUtils.getCanonicalName(eventType);
        synchronized (NotifyCenter.class) {
            INSTANCE.publisherMap.putIfAbsent(topic, publisher);
        }
    }
    
    /**
     * Deregister publisher.
     *
     * @param eventType class Instances type of the event type.
     */
    public static void deregisterPublisher(final Class<? extends Event> eventType) {
        final String topic = ClassUtils.getCanonicalName(eventType);
        EventPublisher publisher = INSTANCE.publisherMap.remove(topic);
        try {
            publisher.shutdown();
        } catch (Throwable ex) {
            LOGGER.error("There was an exception when publisher shutdown : ", ex);
        }
    }
    
}
