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

package com.alibaba.nacos.common.task.engine;

import com.alibaba.nacos.common.lifecycle.Closeable;
import com.alibaba.nacos.common.task.NacosTask;
import com.alibaba.nacos.common.task.NacosTaskProcessor;

import java.util.Collection;

/**
 * Nacos task execute engine.
 *
 * @author xiweng.yy
 */
public interface NacosTaskExecuteEngine<T extends NacosTask> extends Closeable {
    
    /**
     * 获取任务大小
     *
     * @return size of task
     */
    int size();
    
    /**
     * 判断任务引擎是否没有任务执行
     *
     * @return true if the execute engine has no task to do, otherwise false
     */
    boolean isEmpty();
    
    /**
     * 往任务引擎中添加处理类
     *
     * @param key           key of task
     * @param taskProcessor task processor
     */
    void addProcessor(Object key, NacosTaskProcessor taskProcessor);
    
    /**
     * 从任务引擎中删除处理类
     *
     * @param key key of task
     */
    void removeProcessor(Object key);
    
    /**
     * 从任务引擎中找到合适的处理类，没有找到的话，将使用默认的处理类
     *
     * @param key key of task
     * @return task processor for task key or default processor if task processor for task key non-exist
     */
    NacosTaskProcessor getProcessor(Object key);
    
    /**
     * 获取所有的处理类key
     *
     * @return collection of processors
     */
    Collection<Object> getAllProcessorKey();
    
    /**
     * 设置默认的处理类
     * task.
     *
     * @param defaultTaskProcessor default task processor
     */
    void setDefaultTaskProcessor(NacosTaskProcessor defaultTaskProcessor);
    
    /**
     * 往引擎中添加任务
     *
     * @param key  key of task
     * @param task task
     */
    void addTask(Object key, T task);
    
    /**
     * 从引擎中删除任务
     *
     * @param key key of task
     * @return nacos task
     */
    T removeTask(Object key);
    
    /**
     * 获取所有的任务Key
     *
     * @return collection of task keys.
     */
    Collection<Object> getAllTaskKeys();
}
