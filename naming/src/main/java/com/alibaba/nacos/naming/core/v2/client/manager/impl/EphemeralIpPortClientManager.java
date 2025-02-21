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

package com.alibaba.nacos.naming.core.v2.client.manager.impl;

import com.alibaba.nacos.api.common.Constants;
import com.alibaba.nacos.common.notify.NotifyCenter;
import com.alibaba.nacos.naming.consistency.ephemeral.distro.v2.DistroClientVerifyInfo;
import com.alibaba.nacos.naming.constants.ClientConstants;
import com.alibaba.nacos.naming.core.DistroMapper;
import com.alibaba.nacos.naming.core.v2.client.Client;
import com.alibaba.nacos.naming.core.v2.client.ClientAttributes;
import com.alibaba.nacos.naming.core.v2.client.factory.ClientFactory;
import com.alibaba.nacos.naming.core.v2.client.factory.ClientFactoryHolder;
import com.alibaba.nacos.naming.core.v2.client.impl.IpPortBasedClient;
import com.alibaba.nacos.naming.core.v2.client.manager.ClientManager;
import com.alibaba.nacos.naming.core.v2.event.client.ClientEvent;
import com.alibaba.nacos.naming.core.v2.event.client.ClientOperationEvent;
import com.alibaba.nacos.naming.healthcheck.heartbeat.ClientBeatUpdateTask;
import com.alibaba.nacos.naming.misc.ClientConfig;
import com.alibaba.nacos.naming.misc.GlobalExecutor;
import com.alibaba.nacos.naming.misc.Loggers;
import com.alibaba.nacos.naming.misc.NamingExecuteTaskDispatcher;
import com.alibaba.nacos.naming.misc.SwitchDomain;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

/**
 * 短暂临时的客户端
 *
 * @author xiweng.yy
 */
@DependsOn("clientServiceIndexesManager")
@Component("ephemeralIpPortClientManager")
public class EphemeralIpPortClientManager implements ClientManager {

    /**
     * 通过map存储clientId和client之间的映射关系
     *
     * key: IP:端口+true
     * value: 客户端Id、客户端IP，端口等信息，里面还有一些关于客户端心跳、健康检查的一些任务
     */
    private final ConcurrentMap<String, IpPortBasedClient> clients = new ConcurrentHashMap<>();
    
    private final DistroMapper distroMapper;
    
    private final ClientFactory<IpPortBasedClient> clientFactory;
    
    public EphemeralIpPortClientManager(DistroMapper distroMapper, SwitchDomain switchDomain) {
        this.distroMapper = distroMapper;
        // 初始化一个每隔5s执行一次的过期客户端清理任务，关注ExpiredClientCleaner#run方法
        GlobalExecutor.scheduleExpiredClientCleaner(new ExpiredClientCleaner(this, switchDomain), 0,
                Constants.DEFAULT_HEART_BEAT_INTERVAL, TimeUnit.MILLISECONDS);
        clientFactory = ClientFactoryHolder.getInstance().findClientFactory(ClientConstants.EPHEMERAL_IP_PORT);
    }
    
    @Override
    public boolean clientConnected(String clientId, ClientAttributes attributes) {
        return clientConnected(clientFactory.newClient(clientId, attributes));
    }
    
    @Override
    public boolean clientConnected(final Client client) {
        // 向clients这个存储客户端的map中添加客户端信息，并执行客户端的初始化
        clients.computeIfAbsent(client.getClientId(), s -> {
            Loggers.SRV_LOG.info("Client connection {} connect", client.getClientId());
            IpPortBasedClient ipPortBasedClient = (IpPortBasedClient) client;
            // 初始化客户端：
            // 针对临时客户端，启动心跳检查任务
            // 针对持久客户端，启动健康检查任务
            ipPortBasedClient.init();
            return ipPortBasedClient;
        });
        return true;
    }
    
    @Override
    public boolean syncClientConnected(String clientId, ClientAttributes attributes) {
        return clientConnected(clientFactory.newSyncedClient(clientId, attributes));
    }
    
    @Override
    public boolean clientDisconnected(String clientId) {
        Loggers.SRV_LOG.info("Client connection {} disconnect, remove instances and subscribers", clientId);
        IpPortBasedClient client = clients.remove(clientId);
        if (null == client) {
            return true;
        }
        boolean isResponsible = isResponsibleClient(client);
        NotifyCenter.publishEvent(new ClientEvent.ClientDisconnectEvent(client, isResponsible));
        client.release();
        NotifyCenter.publishEvent(new ClientOperationEvent.ClientReleaseEvent(client, isResponsible));
        return true;
    }
    
    @Override
    public Client getClient(String clientId) {
        return clients.get(clientId);
    }
    
    @Override
    public boolean contains(String clientId) {
        return clients.containsKey(clientId);
    }
    
    @Override
    public Collection<String> allClientId() {
        return clients.keySet();
    }
    
    @Override
    public boolean isResponsibleClient(Client client) {
        if (client instanceof IpPortBasedClient) {
            return distroMapper.responsible(((IpPortBasedClient) client).getResponsibleId());
        }
        return false;
    }
    
    @Override
    public boolean verifyClient(DistroClientVerifyInfo verifyData) {
        String clientId = verifyData.getClientId();
        IpPortBasedClient client = clients.get(clientId);
        if (null != client) {
            // remote node of old version will always verify with zero revision
            if (0 == verifyData.getRevision() || client.getRevision() == verifyData.getRevision()) {
                NamingExecuteTaskDispatcher.getInstance()
                        .dispatchAndExecuteTask(clientId, new ClientBeatUpdateTask(client));
                return true;
            } else {
                Loggers.DISTRO.info("[DISTRO-VERIFY-FAILED] IpPortBasedClient[{}] revision local={}, remote={}",
                        client.getClientId(), client.getRevision(), verifyData.getRevision());
            }
        }
        return false;
    }
    
    private static class ExpiredClientCleaner implements Runnable {
        
        private final EphemeralIpPortClientManager clientManager;
        
        private final SwitchDomain switchDomain;
        
        public ExpiredClientCleaner(EphemeralIpPortClientManager clientManager, SwitchDomain switchDomain) {
            this.clientManager = clientManager;
            this.switchDomain = switchDomain;
        }
        
        @Override
        public void run() {
            long currentTime = System.currentTimeMillis();
            // 遍历拿到所有的客户端的信息，校验是否过期，如果过期，通过clientManager将这个客户端移除
            for (String each : clientManager.allClientId()) {
                IpPortBasedClient client = (IpPortBasedClient) clientManager.getClient(each);
                if (null != client && isExpireClient(currentTime, client)) {
                    clientManager.clientDisconnected(each);
                }
            }
        }
        
        private boolean isExpireClient(long currentTime, IpPortBasedClient client) {
            long noUpdatedTime = currentTime - client.getLastUpdatedTime();
            return client.isEphemeral() && (
                    isExpirePublishedClient(noUpdatedTime, client) && isExpireSubscriberClient(noUpdatedTime, client)
                            || noUpdatedTime > ClientConfig.getInstance().getClientExpiredTime());
        }
        
        private boolean isExpirePublishedClient(long noUpdatedTime, IpPortBasedClient client) {
            return client.getAllPublishedService().isEmpty() && noUpdatedTime > Constants.DEFAULT_IP_DELETE_TIMEOUT;
        }
        
        private boolean isExpireSubscriberClient(long noUpdatedTime, IpPortBasedClient client) {
            return client.getAllSubscribeService().isEmpty() || noUpdatedTime > switchDomain.getDefaultPushCacheMillis();
        }
    }
}
