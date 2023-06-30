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

package com.alibaba.nacos.naming.core;

import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.exception.api.NacosApiException;
import com.alibaba.nacos.api.model.v2.ErrorCode;
import com.alibaba.nacos.api.naming.NamingResponseCode;
import com.alibaba.nacos.api.naming.PreservedMetadataKeys;
import com.alibaba.nacos.api.naming.pojo.Instance;
import com.alibaba.nacos.api.naming.pojo.ServiceInfo;
import com.alibaba.nacos.api.naming.utils.NamingUtils;
import com.alibaba.nacos.common.utils.ConvertUtils;
import com.alibaba.nacos.common.utils.InternetAddressUtil;
import com.alibaba.nacos.naming.core.v2.ServiceManager;
import com.alibaba.nacos.naming.core.v2.client.Client;
import com.alibaba.nacos.naming.core.v2.client.ClientAttributes;
import com.alibaba.nacos.naming.core.v2.client.impl.IpPortBasedClient;
import com.alibaba.nacos.naming.core.v2.client.manager.ClientManager;
import com.alibaba.nacos.naming.core.v2.client.manager.ClientManagerDelegate;
import com.alibaba.nacos.naming.core.v2.index.ServiceStorage;
import com.alibaba.nacos.naming.core.v2.metadata.InstanceMetadata;
import com.alibaba.nacos.naming.core.v2.metadata.NamingMetadataManager;
import com.alibaba.nacos.naming.core.v2.metadata.NamingMetadataOperateService;
import com.alibaba.nacos.naming.core.v2.metadata.ServiceMetadata;
import com.alibaba.nacos.naming.core.v2.pojo.InstancePublishInfo;
import com.alibaba.nacos.naming.core.v2.pojo.Service;
import com.alibaba.nacos.naming.core.v2.service.ClientOperationService;
import com.alibaba.nacos.naming.core.v2.service.ClientOperationServiceProxy;
import com.alibaba.nacos.naming.healthcheck.HealthCheckReactor;
import com.alibaba.nacos.naming.healthcheck.RsInfo;
import com.alibaba.nacos.naming.healthcheck.heartbeat.ClientBeatProcessorV2;
import com.alibaba.nacos.naming.misc.Loggers;
import com.alibaba.nacos.naming.misc.SwitchDomain;
import com.alibaba.nacos.naming.misc.UtilsAndCommons;
import com.alibaba.nacos.naming.pojo.InstanceOperationInfo;
import com.alibaba.nacos.naming.pojo.Subscriber;
import com.alibaba.nacos.naming.pojo.instance.BeatInfoInstanceBuilder;
import com.alibaba.nacos.naming.push.UdpPushService;
import com.alibaba.nacos.naming.utils.ServiceUtil;
import com.alibaba.nacos.naming.web.ClientAttributesFilter;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Instance service.
 *
 * @author xiweng.yy
 */
@org.springframework.stereotype.Service
public class InstanceOperatorClientImpl implements InstanceOperator {
    
    private final ClientManager clientManager;
    
    private final ClientOperationService clientOperationService;
    
    private final ServiceStorage serviceStorage;
    
    private final NamingMetadataOperateService metadataOperateService;
    
    private final NamingMetadataManager metadataManager;
    
    private final SwitchDomain switchDomain;
    
    private final UdpPushService pushService;
    
    public InstanceOperatorClientImpl(ClientManagerDelegate clientManager,
            ClientOperationServiceProxy clientOperationService, ServiceStorage serviceStorage,
            NamingMetadataOperateService metadataOperateService, NamingMetadataManager metadataManager,
            SwitchDomain switchDomain, UdpPushService pushService) {
        this.clientManager = clientManager;
        this.clientOperationService = clientOperationService;
        this.serviceStorage = serviceStorage;
        this.metadataOperateService = metadataOperateService;
        this.metadataManager = metadataManager;
        this.switchDomain = switchDomain;
        this.pushService = pushService;
    }
    
    /**
     * This method creates {@code IpPortBasedClient} if it doesn't exist.
     */
    @Override
    public void registerInstance(String namespaceId, String serviceName, Instance instance) throws NacosException {
        // 实例活动状态检查  即实例需要满足：心跳超时时间 > 心跳间隔、IP 删除超时时间 > 心跳间隔
        NamingUtils.checkInstanceIsLegal(instance);

        // 是否临时实例，默认为true
        boolean ephemeral = instance.isEphemeral();

        // instance.toInetAddr()返回IP+":"+端口号，在本例中clientId=172.110.0.138:1001#true
        // 获取客户端ID
        String clientId = IpPortBasedClient.getClientId(instance.toInetAddr(), ephemeral);

        // 向clientManager中注册客户端，后面会获取进行使用的,其实这就是客户端管理的地方
        createIpPortClientIfAbsent(clientId);

        // 通过namespaceId+组名+服务名称+是否临时实例，创建出一个Service
        // namespaceId=public
        // serviceName=DEFAULT_GROUP@@discovery-provider
        // ephemeral=true
        Service service = getService(namespaceId, serviceName, ephemeral);

        // 注册服务实例
        clientOperationService.registerInstance(service, instance, clientId);
    }
    
    @Override
    public void removeInstance(String namespaceId, String serviceName, Instance instance) {
        // 是否临时实例信息
        boolean ephemeral = instance.isEphemeral();
        // 获取客户端ID
        String clientId = IpPortBasedClient.getClientId(instance.toInetAddr(), ephemeral);
        if (!clientManager.contains(clientId)) {
            Loggers.SRV_LOG.warn("remove instance from non-exist client: {}", clientId);
            return;
        }
        // 获取服务信息
        Service service = getService(namespaceId, serviceName, ephemeral);
        // 下线实例
        // 将当前下线实例信息，从发布者集合中移除出去，之后发布事件告知其他订阅者
        clientOperationService.deregisterInstance(service, instance, clientId);
    }
    
    @Override
    public void updateInstance(String namespaceId, String serviceName, Instance instance) throws NacosException {
        NamingUtils.checkInstanceIsLegal(instance);
        
        Service service = getService(namespaceId, serviceName, instance.isEphemeral());
        if (!ServiceManager.getInstance().containSingleton(service)) {
            throw new NacosApiException(NacosException.INVALID_PARAM, ErrorCode.INSTANCE_ERROR,
                    "service not found, namespace: " + namespaceId + ", service: " + service);
        }
        String metadataId = InstancePublishInfo
                .genMetadataId(instance.getIp(), instance.getPort(), instance.getClusterName());
        metadataOperateService.updateInstanceMetadata(service, metadataId, buildMetadata(instance));
    }
    
    private InstanceMetadata buildMetadata(Instance instance) {
        InstanceMetadata result = new InstanceMetadata();
        result.setEnabled(instance.isEnabled());
        result.setWeight(instance.getWeight());
        result.getExtendData().putAll(instance.getMetadata());
        return result;
    }
    
    @Override
    public void patchInstance(String namespaceId, String serviceName, InstancePatchObject patchObject)
            throws NacosException {
        Service service = getService(namespaceId, serviceName, true);
        Instance instance = getInstance(namespaceId, serviceName, patchObject.getCluster(), patchObject.getIp(),
                patchObject.getPort());
        String metadataId = InstancePublishInfo
                .genMetadataId(instance.getIp(), instance.getPort(), instance.getClusterName());
        Optional<InstanceMetadata> instanceMetadata = metadataManager.getInstanceMetadata(service, metadataId);
        InstanceMetadata newMetadata = instanceMetadata.map(this::cloneMetadata).orElseGet(InstanceMetadata::new);
        mergeMetadata(newMetadata, patchObject);
        metadataOperateService.updateInstanceMetadata(service, metadataId, newMetadata);
    }
    
    private InstanceMetadata cloneMetadata(InstanceMetadata instanceMetadata) {
        InstanceMetadata result = new InstanceMetadata();
        result.setExtendData(new HashMap<>(instanceMetadata.getExtendData()));
        result.setWeight(instanceMetadata.getWeight());
        result.setEnabled(instanceMetadata.isEnabled());
        return result;
    }
    
    private void mergeMetadata(InstanceMetadata newMetadata, InstancePatchObject patchObject) {
        if (null != patchObject.getMetadata()) {
            newMetadata.setExtendData(new HashMap<>(patchObject.getMetadata()));
        }
        if (null != patchObject.getEnabled()) {
            newMetadata.setEnabled(patchObject.getEnabled());
        }
        if (null != patchObject.getWeight()) {
            newMetadata.setWeight(patchObject.getWeight());
        }
    }
    
    @Override
    public ServiceInfo listInstance(String namespaceId, String serviceName, Subscriber subscriber, String cluster,
            boolean healthOnly) {
        Service service = getService(namespaceId, serviceName, true);
        // For adapt 1.X subscribe logic
        if (subscriber.getPort() > 0 && pushService.canEnablePush(subscriber.getAgent())) {
            String clientId = IpPortBasedClient.getClientId(subscriber.getAddrStr(), true);
            createIpPortClientIfAbsent(clientId);
            clientOperationService.subscribeService(service, subscriber, clientId);
        }
        ServiceInfo serviceInfo = serviceStorage.getData(service);
        ServiceMetadata serviceMetadata = metadataManager.getServiceMetadata(service).orElse(null);
        ServiceInfo result = ServiceUtil
                .selectInstancesWithHealthyProtection(serviceInfo, serviceMetadata, cluster, healthOnly, true, subscriber.getIp());
        // adapt for v1.x sdk
        result.setName(NamingUtils.getGroupedName(result.getName(), result.getGroupName()));
        return result;
    }
    
    @Override
    public Instance getInstance(String namespaceId, String serviceName, String cluster, String ip, int port)
            throws NacosException {
        Service service = getService(namespaceId, serviceName, true);
        return getInstance0(service, cluster, ip, port);
    }
    
    private Instance getInstance0(Service service, String cluster, String ip, int port) throws NacosException {
        ServiceInfo serviceInfo = serviceStorage.getData(service);
        if (serviceInfo.getHosts().isEmpty()) {
            throw new NacosApiException(NacosException.NOT_FOUND, ErrorCode.RESOURCE_NOT_FOUND,
                    "no ips found for cluster " + cluster + " in service " + service.getGroupedServiceName());
        }
        for (Instance each : serviceInfo.getHosts()) {
            if (cluster.equals(each.getClusterName()) && ip.equals(each.getIp()) && port == each.getPort()) {
                return each;
            }
        }
        throw new NacosApiException(NacosException.NOT_FOUND, ErrorCode.RESOURCE_NOT_FOUND, "no matched ip found!");
    }
    
    @Override
    public int handleBeat(String namespaceId, String serviceName, String ip, int port, String cluster,
            RsInfo clientBeat, BeatInfoInstanceBuilder builder) throws NacosException {
        // 从缓存中获取服务数据，此时缓存中正常情况下一定会存在服务数据
        Service service = getService(namespaceId, serviceName, true);
        // 获取客户端ID
        String clientId = IpPortBasedClient.getClientId(ip + InternetAddressUtil.IP_PORT_SPLITER + port, true);
        // 根据客户端ID信息从 clientManager 中获取注册的实例
        IpPortBasedClient client = (IpPortBasedClient) clientManager.getClient(clientId);
        // 如果 client 为空或者发布者 publishers 信息集合里面没有该客户端实例那么就从新注册一个
        if (null == client || !client.getAllPublishedService().contains(service)) {
            if (null == clientBeat) {
                // 如果心跳实体信息为空返回 20404，即请求未找到
                return NamingResponseCode.RESOURCE_NOT_FOUND;
            }
            Instance instance = builder.setBeatInfo(clientBeat).setServiceName(serviceName).build();
            // 发起服务实例注册
            registerInstance(namespaceId, serviceName, instance);
            // 注册完后再一次从clientManager中获取 client，因为注册的时候就是把这个客户端放入到clientManager中去的
            client = (IpPortBasedClient) clientManager.getClient(clientId);
        }

        // 注册时候已经将当前的 service放入到了 ServiceManager.getInstance()的singletonRepository这个ConcurrentHashMap中，
        // 所以containSingleton正常肯定是要返回true的，否则抛出异常
        if (!ServiceManager.getInstance().containSingleton(service)) {
            throw new NacosException(NacosException.SERVER_ERROR,
                    "service not found: " + serviceName + "@" + namespaceId);
        }
        if (null == clientBeat) {
            clientBeat = new RsInfo();
            clientBeat.setIp(ip);
            clientBeat.setPort(port);
            clientBeat.setCluster(cluster);
            clientBeat.setServiceName(serviceName);
        }
        // 创建一个客户端心跳处理器实例，实现了runnable接口，关注run()方法
        ClientBeatProcessorV2 beatProcessor = new ClientBeatProcessorV2(namespaceId, clientBeat, client);
        // 立即开启调度任务
        HealthCheckReactor.scheduleNow(beatProcessor);
        client.setLastUpdatedTime();
        return NamingResponseCode.OK;
    }
    
    @Override
    public long getHeartBeatInterval(String namespaceId, String serviceName, String ip, int port, String cluster) {
        Service service = getService(namespaceId, serviceName, true);
        String metadataId = InstancePublishInfo.genMetadataId(ip, port, cluster);
        Optional<InstanceMetadata> metadata = metadataManager.getInstanceMetadata(service, metadataId);
        if (metadata.isPresent() && metadata.get().getExtendData()
                .containsKey(PreservedMetadataKeys.HEART_BEAT_INTERVAL)) {
            return ConvertUtils.toLong(metadata.get().getExtendData().get(PreservedMetadataKeys.HEART_BEAT_INTERVAL));
        }
        String clientId = IpPortBasedClient.getClientId(ip + InternetAddressUtil.IP_PORT_SPLITER + port, true);
        Client client = clientManager.getClient(clientId);
        InstancePublishInfo instance = null != client ? client.getInstancePublishInfo(service) : null;
        if (null != instance && instance.getExtendDatum().containsKey(PreservedMetadataKeys.HEART_BEAT_INTERVAL)) {
            return ConvertUtils.toLong(instance.getExtendDatum().get(PreservedMetadataKeys.HEART_BEAT_INTERVAL));
        }
        return switchDomain.getClientBeatInterval();
    }
    
    @Override
    public List<? extends Instance> listAllInstances(String namespaceId, String serviceName) throws NacosException {
        Service service = getService(namespaceId, serviceName, true);
        return serviceStorage.getData(service).getHosts();
    }
    
    @Override
    public List<String> batchUpdateMetadata(String namespaceId, InstanceOperationInfo instanceOperationInfo,
            Map<String, String> metadata) throws NacosException {
        boolean isEphemeral = !UtilsAndCommons.PERSIST.equals(instanceOperationInfo.getConsistencyType());
        String serviceName = instanceOperationInfo.getServiceName();
        Service service = getService(namespaceId, serviceName, isEphemeral);
        List<String> result = new LinkedList<>();
        List<Instance> needUpdateInstance = findBatchUpdateInstance(instanceOperationInfo, service);
        for (Instance each : needUpdateInstance) {
            String metadataId = InstancePublishInfo.genMetadataId(each.getIp(), each.getPort(), each.getClusterName());
            Optional<InstanceMetadata> instanceMetadata = metadataManager.getInstanceMetadata(service, metadataId);
            InstanceMetadata newMetadata = instanceMetadata.map(this::cloneMetadata).orElseGet(InstanceMetadata::new);
            newMetadata.getExtendData().putAll(metadata);
            metadataOperateService.updateInstanceMetadata(service, metadataId, newMetadata);
            result.add(each.toInetAddr() + ":" + UtilsAndCommons.LOCALHOST_SITE + ":" + each.getClusterName() + ":" + (
                    each.isEphemeral() ? UtilsAndCommons.EPHEMERAL : UtilsAndCommons.PERSIST));
        }
        return result;
    }
    
    @Override
    public List<String> batchDeleteMetadata(String namespaceId, InstanceOperationInfo instanceOperationInfo,
            Map<String, String> metadata) throws NacosException {
        boolean isEphemeral = !UtilsAndCommons.PERSIST.equals(instanceOperationInfo.getConsistencyType());
        String serviceName = instanceOperationInfo.getServiceName();
        Service service = getService(namespaceId, serviceName, isEphemeral);
        List<String> result = new LinkedList<>();
        List<Instance> needUpdateInstance = findBatchUpdateInstance(instanceOperationInfo, service);
        for (Instance each : needUpdateInstance) {
            String metadataId = InstancePublishInfo.genMetadataId(each.getIp(), each.getPort(), each.getClusterName());
            Optional<InstanceMetadata> instanceMetadata = metadataManager.getInstanceMetadata(service, metadataId);
            InstanceMetadata newMetadata = instanceMetadata.map(this::cloneMetadata).orElseGet(InstanceMetadata::new);
            metadata.keySet().forEach(key -> newMetadata.getExtendData().remove(key));
            metadataOperateService.updateInstanceMetadata(service, metadataId, newMetadata);
            result.add(each.toInetAddr() + ":" + UtilsAndCommons.LOCALHOST_SITE + ":" + each.getClusterName() + ":" + (
                    each.isEphemeral() ? UtilsAndCommons.EPHEMERAL : UtilsAndCommons.PERSIST));
        }
        return result;
    }
    
    private List<Instance> findBatchUpdateInstance(InstanceOperationInfo instanceOperationInfo, Service service) {
        if (null == instanceOperationInfo.getInstances() || instanceOperationInfo.getInstances().isEmpty()) {
            return serviceStorage.getData(service).getHosts();
        }
        List<Instance> result = new LinkedList<>();
        for (Instance each : instanceOperationInfo.getInstances()) {
            try {
                getInstance0(service, each.getClusterName(), each.getIp(), each.getPort());
                result.add(each);
            } catch (NacosException ignored) {
            }
        }
        return result;
    }
    
    private void createIpPortClientIfAbsent(String clientId) {
        if (!clientManager.contains(clientId)) {
            ClientAttributes clientAttributes;
            if (ClientAttributesFilter.threadLocalClientAttributes.get() != null) {
                clientAttributes = ClientAttributesFilter.threadLocalClientAttributes.get();
            } else {
                clientAttributes = new ClientAttributes();
            }
            clientManager.clientConnected(clientId, clientAttributes);
        }
    }
    
    private Service getService(String namespaceId, String serviceName, boolean ephemeral) {
        // 获取服务组名DEFAULT_GROUP
        String groupName = NamingUtils.getGroupName(serviceName);
        // 获取服务名称discovery-provider
        String serviceNameNoGrouped = NamingUtils.getServiceName(serviceName);
        // new Service(namespace, group, name, ephemeral)
        return Service.newService(namespaceId, groupName, serviceNameNoGrouped, ephemeral);
    }
    
}
