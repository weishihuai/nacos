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

package com.alibaba.nacos.core.cluster;

import com.alibaba.nacos.api.exception.NacosException;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Addressable pattern base class.
 *
 * @author <a href="mailto:liaochuntao@live.com">liaochuntao</a>
 */
public abstract class AbstractMemberLookup implements MemberLookup {
    
    protected ServerMemberManager memberManager;
    
    protected AtomicBoolean start = new AtomicBoolean(false);
    
    @Override
    public void injectMemberManager(ServerMemberManager memberManager) {
        this.memberManager = memberManager;
    }
    
    @Override
    public void afterLookup(Collection<Member> members) {
        // 当成员列表实例化完成后，会通过订阅发布模式将MembersChangeEvent放入DefaultPublisher的队列中。
        this.memberManager.memberChange(members);
    }
    
    @Override
    public void destroy() throws NacosException {
        if (start.compareAndSet(true, false)) {
            doDestroy();
        }
    }
    
    @Override
    public void start() throws NacosException {
        // 未启动则启动
        if (start.compareAndSet(false, true)) {
            /**
             * 1. 对于单机模式, 直接获取本机地址作为集群列表
             * 2. 对于FileConfigMemberLookup, 首先读取配置文件中的成员列表，然后通过MemberUtil工具类的singleParse方法构造Member对象且默认状态为UP
             */
            doStart();
        }
    }
    
    /**
     * subclass can override this method if need.
     * @throws NacosException NacosException
     */
    protected abstract void doStart() throws NacosException;
    
    /**
     * subclass can override this method if need.
     * @throws NacosException nacosException
     */
    protected abstract void doDestroy() throws NacosException;
}
