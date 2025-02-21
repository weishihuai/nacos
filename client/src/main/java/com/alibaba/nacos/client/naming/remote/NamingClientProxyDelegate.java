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

package com.alibaba.nacos.client.naming.remote;

import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.naming.pojo.Instance;
import com.alibaba.nacos.api.naming.pojo.ListView;
import com.alibaba.nacos.api.naming.pojo.Service;
import com.alibaba.nacos.api.naming.pojo.ServiceInfo;
import com.alibaba.nacos.api.naming.utils.NamingUtils;
import com.alibaba.nacos.api.selector.AbstractSelector;
import com.alibaba.nacos.client.env.NacosClientProperties;
import com.alibaba.nacos.client.naming.cache.ServiceInfoHolder;
import com.alibaba.nacos.client.naming.core.ServerListManager;
import com.alibaba.nacos.client.naming.core.ServiceInfoUpdateService;
import com.alibaba.nacos.client.naming.event.InstancesChangeNotifier;
import com.alibaba.nacos.client.naming.remote.gprc.NamingGrpcClientProxy;
import com.alibaba.nacos.client.naming.remote.http.NamingHttpClientManager;
import com.alibaba.nacos.client.naming.remote.http.NamingHttpClientProxy;
import com.alibaba.nacos.client.security.SecurityProxy;
import com.alibaba.nacos.common.utils.CollectionUtils;
import com.alibaba.nacos.common.utils.ThreadUtils;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.alibaba.nacos.client.constant.Constants.Security.SECURITY_INFO_REFRESH_INTERVAL_MILLS;
import static com.alibaba.nacos.client.utils.LogUtils.NAMING_LOGGER;

/**
 * 代理委托类：这个类并不是真正的代理类，真正的代理类是grpcClientProxy和httpClientProxy，
 * 这个类仅仅是做了一个委托功能，将处理的方法委托给了这两个代理类去处理。根据实际情况选择http或gRPC协议请求服务端。
 *
 * Delegate of naming client proxy.
 *
 * @author xiweng.yy
 */
public class NamingClientProxyDelegate implements NamingClientProxy {
    
    private final ServerListManager serverListManager;
    
    private final ServiceInfoUpdateService serviceInfoUpdateService;
    
    private final ServiceInfoHolder serviceInfoHolder;
    
    private final NamingHttpClientProxy httpClientProxy;
    
    private final NamingGrpcClientProxy grpcClientProxy;
    
    private final SecurityProxy securityProxy;
    
    private ScheduledExecutorService executorService;
    
    public NamingClientProxyDelegate(String namespace, ServiceInfoHolder serviceInfoHolder, NacosClientProperties properties,
            InstancesChangeNotifier changeNotifier) throws NacosException {
        this.serviceInfoUpdateService = new ServiceInfoUpdateService(properties, serviceInfoHolder, this,
                changeNotifier);
        this.serverListManager = new ServerListManager(properties, namespace);
        this.serviceInfoHolder = serviceInfoHolder;
        this.securityProxy = new SecurityProxy(this.serverListManager.getServerList(),
                NamingHttpClientManager.getInstance().getNacosRestTemplate());
        initSecurityProxy(properties);

        // 真正的代理类： httpClientProxy、grpcClientProxy
        this.httpClientProxy = new NamingHttpClientProxy(namespace, securityProxy, serverListManager, properties);
        this.grpcClientProxy = new NamingGrpcClientProxy(namespace, securityProxy, serverListManager, properties,
                serviceInfoHolder);
    }
    
    private void initSecurityProxy(NacosClientProperties properties) {
        this.executorService = new ScheduledThreadPoolExecutor(1, r -> {
            Thread t = new Thread(r);
            t.setName("com.alibaba.nacos.client.naming.security");
            t.setDaemon(true);
            return t;
        });
        final Properties nacosClientPropertiesView = properties.asProperties();
        this.securityProxy.login(nacosClientPropertiesView);
        this.executorService
                .scheduleWithFixedDelay(() -> securityProxy.login(nacosClientPropertiesView), 0, SECURITY_INFO_REFRESH_INTERVAL_MILLS,
                        TimeUnit.MILLISECONDS);
    }
    
    @Override
    public void registerService(String serviceName, String groupName, Instance instance) throws NacosException {
        // 根据是否是临时实例，选择不同的代理类去注册   grpcClientProxy or  httpClientProxy
        getExecuteClientProxy(instance).registerService(serviceName, groupName, instance);
    }
    
    @Override
    public void batchRegisterService(String serviceName, String groupName, List<Instance> instances)
            throws NacosException {
        NAMING_LOGGER.info("batchRegisterInstance instances: {} ,serviceName: {} begin.", instances, serviceName);
        if (CollectionUtils.isEmpty(instances)) {
            NAMING_LOGGER.warn("batchRegisterInstance instances is Empty:{}", instances);
        }
        grpcClientProxy.batchRegisterService(serviceName, groupName, instances);
        NAMING_LOGGER.info("batchRegisterInstance instances: {} ,serviceName: {} finish.", instances, serviceName);
    }
    
    @Override
    public void batchDeregisterService(String serviceName, String groupName, List<Instance> instances)
            throws NacosException {
        NAMING_LOGGER.info("batch DeregisterInstance instances: {} ,serviceName: {} begin.", instances, serviceName);
        if (CollectionUtils.isEmpty(instances)) {
            NAMING_LOGGER.warn("batch DeregisterInstance instances is Empty:{}", instances);
        }
        grpcClientProxy.batchDeregisterService(serviceName, groupName, instances);
        NAMING_LOGGER.info("batch DeregisterInstance instances: {} ,serviceName: {} finish.", instances, serviceName);
    }
    
    @Override
    public void deregisterService(String serviceName, String groupName, Instance instance) throws NacosException {
        getExecuteClientProxy(instance).deregisterService(serviceName, groupName, instance);
    }
    
    @Override
    public void updateInstance(String serviceName, String groupName, Instance instance) throws NacosException {
    
    }
    
    @Override
    public ServiceInfo queryInstancesOfService(String serviceName, String groupName, String clusters, int udpPort,
            boolean healthyOnly) throws NacosException {
        return grpcClientProxy.queryInstancesOfService(serviceName, groupName, clusters, udpPort, healthyOnly);
    }
    
    @Override
    public Service queryService(String serviceName, String groupName) throws NacosException {
        return null;
    }
    
    @Override
    public void createService(Service service, AbstractSelector selector) throws NacosException {
    
    }
    
    @Override
    public boolean deleteService(String serviceName, String groupName) throws NacosException {
        return false;
    }
    
    @Override
    public void updateService(Service service, AbstractSelector selector) throws NacosException {
    
    }
    
    @Override
    public ListView<String> getServiceList(int pageNo, int pageSize, String groupName, AbstractSelector selector)
            throws NacosException {
        return grpcClientProxy.getServiceList(pageNo, pageSize, groupName, selector);
    }
    
    @Override
    public ServiceInfo subscribe(String serviceName, String groupName, String clusters) throws NacosException {
        NAMING_LOGGER.info("[SUBSCRIBE-SERVICE] service:{}, group:{}, clusters:{} ", serviceName, groupName, clusters);
        // 服务名称（带组名）  格式：groupName@@serviceName
        String serviceNameWithGroup = NamingUtils.getGroupedName(serviceName, groupName);
        // 如果集群名称非空，拼接上clusters
        String serviceKey = ServiceInfo.getKey(serviceNameWithGroup, clusters);
        // 调度更新，往线程池中提交一个UpdateTask任务，持续更新订阅服务的注册表
        // 这个定时任务主要的作用就是来定时同步服务器端的实例列表信息，并进行本地缓存更新等操作。
        serviceInfoUpdateService.scheduleUpdateIfAbsent(serviceName, groupName, clusters);
        // 获取缓存中的服务信息
        ServiceInfo result = serviceInfoHolder.getServiceInfoMap().get(serviceKey);
        if (null == result || !isSubscribed(serviceName, groupName, clusters)) {
            // 缓存中不存在对应的服务信息 或者 SubscriberRedoData还未注册，则执行订阅
            result = grpcClientProxy.subscribe(serviceName, groupName, clusters);
        }
        // 处理服务信息：获取本地内存中旧的ServiceInfo服务信息，将最新的ServiceInfo服务信息重新存入客户端缓存中，对比新的服务信息，如发生变更，则发布实例变更数据，并同步serviceInfo数据到本地文件
        serviceInfoHolder.processServiceInfo(result);
        return result;
    }
    
    @Override
    public void unsubscribe(String serviceName, String groupName, String clusters) throws NacosException {
        NAMING_LOGGER
                .debug("[UNSUBSCRIBE-SERVICE] service:{}, group:{}, cluster:{} ", serviceName, groupName, clusters);
        serviceInfoUpdateService.stopUpdateIfContain(serviceName, groupName, clusters);
        grpcClientProxy.unsubscribe(serviceName, groupName, clusters);
    }
    
    @Override
    public boolean isSubscribed(String serviceName, String groupName, String clusters) throws NacosException {
        return grpcClientProxy.isSubscribed(serviceName, groupName, clusters);
    }
    
    @Override
    public boolean serverHealthy() {
        return grpcClientProxy.serverHealthy() || httpClientProxy.serverHealthy();
    }
    
    private NamingClientProxy getExecuteClientProxy(Instance instance) {
        // 如果是临时实例，使用grpc方式，否则使用http方式
        // ephemeral默认为true,也就是返回grpcClientProxy,Nacos 2.0版本将http的请求换成了gRpc了

        // 临时instance：gRPC长连接
        // 持久instance：http短连接
        return instance.isEphemeral() ? grpcClientProxy : httpClientProxy;
    }
    
    @Override
    public void shutdown() throws NacosException {
        String className = this.getClass().getName();
        NAMING_LOGGER.info("{} do shutdown begin", className);
        serviceInfoUpdateService.shutdown();
        serverListManager.shutdown();
        httpClientProxy.shutdown();
        grpcClientProxy.shutdown();
        securityProxy.shutdown();
        ThreadUtils.shutdownThreadPool(executorService, NAMING_LOGGER);
        NAMING_LOGGER.info("{} do shutdown stop", className);
    }
}
