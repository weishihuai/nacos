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

package com.alibaba.nacos.naming.consistency.ephemeral.distro.v2;

import com.alibaba.nacos.common.notify.Event;
import com.alibaba.nacos.common.notify.NotifyCenter;
import com.alibaba.nacos.common.notify.listener.SmartSubscriber;
import com.alibaba.nacos.consistency.DataOperation;
import com.alibaba.nacos.core.distributed.distro.DistroProtocol;
import com.alibaba.nacos.core.distributed.distro.component.DistroDataProcessor;
import com.alibaba.nacos.core.distributed.distro.component.DistroDataStorage;
import com.alibaba.nacos.core.distributed.distro.entity.DistroData;
import com.alibaba.nacos.core.distributed.distro.entity.DistroKey;
import com.alibaba.nacos.naming.cluster.transport.Serializer;
import com.alibaba.nacos.naming.constants.ClientConstants;
import com.alibaba.nacos.naming.core.v2.ServiceManager;
import com.alibaba.nacos.naming.core.v2.client.Client;
import com.alibaba.nacos.naming.core.v2.client.ClientSyncData;
import com.alibaba.nacos.naming.core.v2.client.ClientSyncDatumSnapshot;
import com.alibaba.nacos.naming.core.v2.client.manager.ClientManager;
import com.alibaba.nacos.naming.core.v2.event.client.ClientEvent;
import com.alibaba.nacos.naming.core.v2.event.client.ClientOperationEvent;
import com.alibaba.nacos.naming.core.v2.event.publisher.NamingEventPublisherFactory;
import com.alibaba.nacos.naming.core.v2.pojo.BatchInstanceData;
import com.alibaba.nacos.naming.core.v2.pojo.BatchInstancePublishInfo;
import com.alibaba.nacos.naming.core.v2.pojo.InstancePublishInfo;
import com.alibaba.nacos.naming.core.v2.pojo.Service;
import com.alibaba.nacos.naming.misc.Loggers;
import com.alibaba.nacos.sys.env.EnvUtil;
import com.alibaba.nacos.sys.utils.ApplicationUtils;
import org.apache.commons.collections.CollectionUtils;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * 客户端数据一致性处理器
 *
 * @author xiweng.yy
 */
public class DistroClientDataProcessor extends SmartSubscriber implements DistroDataStorage, DistroDataProcessor {
    
    public static final String TYPE = "Nacos:Naming:v2:ClientData";
    
    private final ClientManager clientManager;
    
    private final DistroProtocol distroProtocol;
    
    private volatile boolean isFinishInitial;
    
    public DistroClientDataProcessor(ClientManager clientManager, DistroProtocol distroProtocol) {
        this.clientManager = clientManager;
        this.distroProtocol = distroProtocol;
        NotifyCenter.registerSubscriber(this, NamingEventPublisherFactory.getInstance());
    }
    
    @Override
    public void finishInitial() {
        isFinishInitial = true;
    }
    
    @Override
    public boolean isFinishInitial() {
        return isFinishInitial;
    }
    
    @Override
    public List<Class<? extends Event>> subscribeTypes() {
        List<Class<? extends Event>> result = new LinkedList<>();
        result.add(ClientEvent.ClientChangedEvent.class);
        result.add(ClientEvent.ClientDisconnectEvent.class);
        result.add(ClientEvent.ClientVerifyFailedEvent.class);
        return result;
    }
    
    @Override
    public void onEvent(Event event) {
        // 只有集群模式才有效，单机模式启动的Nacos，不会执行同步操作
        if (EnvUtil.getStandaloneMode()) {
            return;
        }
        if (event instanceof ClientEvent.ClientVerifyFailedEvent) {
            syncToVerifyFailedServer((ClientEvent.ClientVerifyFailedEvent) event);
        } else {
            // 同步数据给其它节点
            syncToAllServer((ClientEvent) event);
        }
    }
    
    private void syncToVerifyFailedServer(ClientEvent.ClientVerifyFailedEvent event) {
        Client client = clientManager.getClient(event.getClientId());
        if (isInvalidClient(client)) {
            return;
        }
        DistroKey distroKey = new DistroKey(client.getClientId(), TYPE);
        // Verify failed data should be sync directly.
        distroProtocol.syncToTarget(distroKey, DataOperation.ADD, event.getTargetServer(), 0L);
    }
    
    private void syncToAllServer(ClientEvent event) {
        Client client = event.getClient();
        /**
         * 判断客户端是否为空，是否是临时实例，判断是否是负责节点
         *
         * 临时实例，使用distro协议同步； 持久实例：使用raft协议同步
         */
        if (isInvalidClient(client)) {
            return;
        }
        if (event instanceof ClientEvent.ClientDisconnectEvent) {
            // 客户端断开连接事件
            DistroKey distroKey = new DistroKey(client.getClientId(), TYPE);
            distroProtocol.sync(distroKey, DataOperation.DELETE);
        } else if (event instanceof ClientEvent.ClientChangedEvent) {
            // 客户端变更事件（新增或修改）
            DistroKey distroKey = new DistroKey(client.getClientId(), TYPE);
            distroProtocol.sync(distroKey, DataOperation.CHANGE);
        }
    }
    
    private boolean isInvalidClient(Client client) {
        // Only ephemeral data sync by Distro, persist client should sync by raft.
        return null == client || !client.isEphemeral() || !clientManager.isResponsibleClient(client);
    }
    
    @Override
    public String processType() {
        return TYPE;
    }
    
    @Override
    public boolean processData(DistroData distroData) {
        switch (distroData.getType()) {
            case ADD:
            case CHANGE:
                // 反序列化同步数据为ClientSyncData
                ClientSyncData clientSyncData = ApplicationUtils.getBean(Serializer.class)
                        .deserialize(distroData.getContent(), ClientSyncData.class);
                // 处理同步数据
                handlerClientSyncData(clientSyncData);
                return true;
            case DELETE:
                String deleteClientId = distroData.getDistroKey().getResourceKey();
                Loggers.DISTRO.info("[Client-Delete] Received distro client sync data {}", deleteClientId);
                clientManager.clientDisconnected(deleteClientId);
                return true;
            default:
                return false;
        }
    }
    
    private void handlerClientSyncData(ClientSyncData clientSyncData) {
        Loggers.DISTRO
                .info("[Client-Add] Received distro client sync data {}, revision={}", clientSyncData.getClientId(),
                        clientSyncData.getAttributes().getClientAttribute(ClientConstants.REVISION, 0L));
        // 同步客户端连接，生成client：不存在时创建client
        clientManager.syncClientConnected(clientSyncData.getClientId(), clientSyncData.getAttributes());
        // 获取Client
        Client client = clientManager.getClient(clientSyncData.getClientId());
        // 更新Client数据
        upgradeClient(client, clientSyncData);
    }
    
    private void upgradeClient(Client client, ClientSyncData clientSyncData) {
        Set<Service> syncedService = new HashSet<>();
        // process batch instance sync logic
        processBatchInstanceDistroData(syncedService, client, clientSyncData);
        List<String> namespaces = clientSyncData.getNamespaces();
        List<String> groupNames = clientSyncData.getGroupNames();
        List<String> serviceNames = clientSyncData.getServiceNames();
        List<InstancePublishInfo> instances = clientSyncData.getInstancePublishInfos();
        
        for (int i = 0; i < namespaces.size(); i++) {
            Service service = Service.newService(namespaces.get(i), groupNames.get(i), serviceNames.get(i));
            Service singleton = ServiceManager.getInstance().getSingleton(service);
            syncedService.add(singleton);
            InstancePublishInfo instancePublishInfo = instances.get(i);
            if (!instancePublishInfo.equals(client.getInstancePublishInfo(singleton))) {
                // 添加注册表信息，并发布ClientRegisterServiceEvent
                client.addServiceInstance(singleton, instancePublishInfo);
                NotifyCenter.publishEvent(
                        new ClientOperationEvent.ClientRegisterServiceEvent(singleton, client.getClientId()));
            }
        }
        for (Service each : client.getAllPublishedService()) {
            if (!syncedService.contains(each)) {
                // 删除注册表信息，并发布ClientDeregisterServiceEvent
                client.removeServiceInstance(each);
                NotifyCenter.publishEvent(
                        new ClientOperationEvent.ClientDeregisterServiceEvent(each, client.getClientId()));
            }
        }
        client.setRevision(clientSyncData.getAttributes().<Integer>getClientAttribute(ClientConstants.REVISION, 0));
    }
    
    private static void processBatchInstanceDistroData(Set<Service> syncedService, Client client,
            ClientSyncData clientSyncData) {
        BatchInstanceData batchInstanceData = clientSyncData.getBatchInstanceData();
        if (batchInstanceData == null || CollectionUtils.isEmpty(batchInstanceData.getNamespaces())) {
            Loggers.DISTRO.info("[processBatchInstanceDistroData] BatchInstanceData is null , clientId is :{}",
                    client.getClientId());
            return;
        }
        List<String> namespaces = batchInstanceData.getNamespaces();
        List<String> groupNames = batchInstanceData.getGroupNames();
        List<String> serviceNames = batchInstanceData.getServiceNames();
        List<BatchInstancePublishInfo> batchInstancePublishInfos = batchInstanceData.getBatchInstancePublishInfos();
        
        for (int i = 0; i < namespaces.size(); i++) {
            Service service = Service.newService(namespaces.get(i), groupNames.get(i), serviceNames.get(i));
            Service singleton = ServiceManager.getInstance().getSingleton(service);
            syncedService.add(singleton);
            BatchInstancePublishInfo batchInstancePublishInfo = batchInstancePublishInfos.get(i);
            BatchInstancePublishInfo targetInstanceInfo = (BatchInstancePublishInfo) client
                    .getInstancePublishInfo(singleton);
            boolean result = false;
            if (targetInstanceInfo != null) {
                result = batchInstancePublishInfo.equals(targetInstanceInfo);
            }
            if (!result) {
                client.addServiceInstance(singleton, batchInstancePublishInfo);
                NotifyCenter.publishEvent(
                        new ClientOperationEvent.ClientRegisterServiceEvent(singleton, client.getClientId()));
            }
        }
    }
    
    @Override
    public boolean processVerifyData(DistroData distroData, String sourceAddress) {
        DistroClientVerifyInfo verifyData = ApplicationUtils.getBean(Serializer.class)
                .deserialize(distroData.getContent(), DistroClientVerifyInfo.class);
        if (clientManager.verifyClient(verifyData)) {
            return true;
        }
        Loggers.DISTRO.info("client {} is invalid, get new client from {}", verifyData.getClientId(), sourceAddress);
        return false;
    }
    
    @Override
    public boolean processSnapshot(DistroData distroData) {
        ClientSyncDatumSnapshot snapshot = ApplicationUtils.getBean(Serializer.class)
                .deserialize(distroData.getContent(), ClientSyncDatumSnapshot.class);
        for (ClientSyncData each : snapshot.getClientSyncDataList()) {
            handlerClientSyncData(each);
        }
        return true;
    }
    
    @Override
    public DistroData getDistroData(DistroKey distroKey) {
        Client client = clientManager.getClient(distroKey.getResourceKey());
        if (null == client) {
            return null;
        }
        // 把生成的同步数据放入到数组中
        byte[] data = ApplicationUtils.getBean(Serializer.class).serialize(client.generateSyncData());
        return new DistroData(distroKey, data);
    }
    
    @Override
    public DistroData getDatumSnapshot() {
        List<ClientSyncData> datum = new LinkedList<>();
        for (String each : clientManager.allClientId()) {
            Client client = clientManager.getClient(each);
            if (null == client || !client.isEphemeral()) {
                continue;
            }
            datum.add(client.generateSyncData());
        }
        ClientSyncDatumSnapshot snapshot = new ClientSyncDatumSnapshot();
        snapshot.setClientSyncDataList(datum);
        byte[] data = ApplicationUtils.getBean(Serializer.class).serialize(snapshot);
        return new DistroData(new DistroKey(DataOperation.SNAPSHOT.name(), TYPE), data);
    }
    
    @Override
    public List<DistroData> getVerifyData() {
        List<DistroData> result = null;
        for (String each : clientManager.allClientId()) {
            Client client = clientManager.getClient(each);
            if (null == client || !client.isEphemeral()) {
                continue;
            }
            if (clientManager.isResponsibleClient(client)) {
                DistroClientVerifyInfo verifyData = new DistroClientVerifyInfo(client.getClientId(),
                        client.getRevision());
                DistroKey distroKey = new DistroKey(client.getClientId(), TYPE);
                DistroData data = new DistroData(distroKey,
                        ApplicationUtils.getBean(Serializer.class).serialize(verifyData));
                data.setType(DataOperation.VERIFY);
                if (result == null) {
                    result = new LinkedList<>();
                }
                result.add(data);
            }
        }
        return result;
    }
}
