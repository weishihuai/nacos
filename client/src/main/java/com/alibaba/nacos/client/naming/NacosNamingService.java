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

package com.alibaba.nacos.client.naming;

import com.alibaba.nacos.api.PropertyKeyConst;
import com.alibaba.nacos.api.common.Constants;
import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.naming.NamingService;
import com.alibaba.nacos.api.naming.listener.EventListener;
import com.alibaba.nacos.api.naming.pojo.Instance;
import com.alibaba.nacos.api.naming.pojo.ListView;
import com.alibaba.nacos.api.naming.pojo.ServiceInfo;
import com.alibaba.nacos.api.naming.utils.NamingUtils;
import com.alibaba.nacos.api.selector.AbstractSelector;
import com.alibaba.nacos.client.env.NacosClientProperties;
import com.alibaba.nacos.client.naming.cache.ServiceInfoHolder;
import com.alibaba.nacos.client.naming.core.Balancer;
import com.alibaba.nacos.client.naming.event.InstancesChangeEvent;
import com.alibaba.nacos.client.naming.event.InstancesChangeNotifier;
import com.alibaba.nacos.client.naming.remote.NamingClientProxy;
import com.alibaba.nacos.client.naming.remote.NamingClientProxyDelegate;
import com.alibaba.nacos.client.naming.utils.CollectionUtils;
import com.alibaba.nacos.client.naming.utils.InitUtils;
import com.alibaba.nacos.client.naming.utils.UtilAndComs;
import com.alibaba.nacos.client.utils.ValidatorUtils;
import com.alibaba.nacos.common.notify.NotifyCenter;
import com.alibaba.nacos.common.utils.StringUtils;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

/**
 * Nacos Naming Service.
 *
 * @author nkorange
 */
@SuppressWarnings("PMD.ServiceOrDaoClassShouldEndWithImplRule")
public class NacosNamingService implements NamingService {
    
    private static final String DEFAULT_NAMING_LOG_FILE_PATH =  "naming.log";
    
    private static final String UP = "UP";
    
    private static final String DOWN = "DOWN";
    
    /**
     * Each Naming service should have different namespace.
     */
    private String namespace;
    
    private String logName;
    
    private ServiceInfoHolder serviceInfoHolder;
    
    private InstancesChangeNotifier changeNotifier;
    
    private NamingClientProxy clientProxy;
    
    private String notifierEventScope;
    
    public NacosNamingService(String serverList) throws NacosException {
        Properties properties = new Properties();
        properties.setProperty(PropertyKeyConst.SERVER_ADDR, serverList);
        init(properties);
    }
    
    public NacosNamingService(Properties properties) throws NacosException {
        init(properties);
    }
    
    private void init(Properties properties) throws NacosException {
        final NacosClientProperties nacosClientProperties = NacosClientProperties.PROTOTYPE.derive(properties);
        
        ValidatorUtils.checkInitParam(nacosClientProperties);
        this.namespace = InitUtils.initNamespaceForNaming(nacosClientProperties);
        InitUtils.initSerialization();
        InitUtils.initWebRootContext(nacosClientProperties);
        initLogName(nacosClientProperties);
    
        this.notifierEventScope = UUID.randomUUID().toString();
        this.changeNotifier = new InstancesChangeNotifier(this.notifierEventScope);
        NotifyCenter.registerToPublisher(InstancesChangeEvent.class, 16384);
        NotifyCenter.registerSubscriber(changeNotifier);
        this.serviceInfoHolder = new ServiceInfoHolder(namespace, this.notifierEventScope, nacosClientProperties);

        // 创建代理类
        this.clientProxy = new NamingClientProxyDelegate(this.namespace, serviceInfoHolder, nacosClientProperties, changeNotifier);
    }
    
    private void initLogName(NacosClientProperties properties) {
        logName = properties.getProperty(UtilAndComs.NACOS_NAMING_LOG_NAME, DEFAULT_NAMING_LOG_FILE_PATH);
    }
    
    @Override
    public void registerInstance(String serviceName, String ip, int port) throws NacosException {
        registerInstance(serviceName, ip, port, Constants.DEFAULT_CLUSTER_NAME);
    }
    
    @Override
    public void registerInstance(String serviceName, String groupName, String ip, int port) throws NacosException {
        registerInstance(serviceName, groupName, ip, port, Constants.DEFAULT_CLUSTER_NAME);
    }
    
    @Override
    public void registerInstance(String serviceName, String ip, int port, String clusterName) throws NacosException {
        registerInstance(serviceName, Constants.DEFAULT_GROUP, ip, port, clusterName);
    }
    
    @Override
    public void registerInstance(String serviceName, String groupName, String ip, int port, String clusterName)
            throws NacosException {
        // 创建了一个Instance实例
        Instance instance = new Instance();
        instance.setIp(ip);
        instance.setPort(port);
        instance.setWeight(1.0);
        instance.setClusterName(clusterName);
        registerInstance(serviceName, groupName, instance);
    }
    
    @Override
    public void registerInstance(String serviceName, Instance instance) throws NacosException {
        registerInstance(serviceName, Constants.DEFAULT_GROUP, instance);
    }
    
    @Override
    public void registerInstance(String serviceName, String groupName, Instance instance) throws NacosException {
        // 检查instance相关属性的合法性
        NamingUtils.checkInstanceIsLegal(instance);

        // 通过客户端代理去注册服务
        // 在构造方法中调用了init()初始化方法，创建了一个clientProxy代理类对象，具体的实现是NamingClientProxyDelegate
        // this.clientProxy = new NamingClientProxyDelegate(this.namespace, serviceInfoHolder, nacosClientProperties, changeNotifier);
        clientProxy.registerService(serviceName, groupName, instance);
    }
    
    @Override
    public void batchRegisterInstance(String serviceName, String groupName, List<Instance> instances)
            throws NacosException {
        NamingUtils.batchCheckInstanceIsLegal(instances);
        clientProxy.batchRegisterService(serviceName, groupName, instances);
    }
    
    @Override
    public void batchDeregisterInstance(String serviceName, String groupName, List<Instance> instances)
            throws NacosException {
        NamingUtils.batchCheckInstanceIsLegal(instances);
        clientProxy.batchDeregisterService(serviceName, groupName, instances);
    }
    
    @Override
    public void deregisterInstance(String serviceName, String ip, int port) throws NacosException {
        // DEFAULT_CLUSTER_NAME = "DEFAULT"
        deregisterInstance(serviceName, ip, port, Constants.DEFAULT_CLUSTER_NAME);
    }
    
    @Override
    public void deregisterInstance(String serviceName, String groupName, String ip, int port) throws NacosException {
        deregisterInstance(serviceName, groupName, ip, port, Constants.DEFAULT_CLUSTER_NAME);
    }
    
    @Override
    public void deregisterInstance(String serviceName, String ip, int port, String clusterName) throws NacosException {
        deregisterInstance(serviceName, Constants.DEFAULT_GROUP, ip, port, clusterName);
    }
    
    @Override
    public void deregisterInstance(String serviceName, String groupName, String ip, int port, String clusterName)
            throws NacosException {
        // 构建一个实例信息
        Instance instance = new Instance();
        instance.setIp(ip);
        instance.setPort(port);
        instance.setClusterName(clusterName);
        deregisterInstance(serviceName, groupName, instance);
    }
    
    @Override
    public void deregisterInstance(String serviceName, Instance instance) throws NacosException {
        deregisterInstance(serviceName, Constants.DEFAULT_GROUP, instance);
    }
    
    @Override
    public void deregisterInstance(String serviceName, String groupName, Instance instance) throws NacosException {
        clientProxy.deregisterService(serviceName, groupName, instance);
    }
    
    @Override
    public List<Instance> getAllInstances(String serviceName) throws NacosException {
        return getAllInstances(serviceName, new ArrayList<>());
    }
    
    @Override
    public List<Instance> getAllInstances(String serviceName, String groupName) throws NacosException {
        return getAllInstances(serviceName, groupName, new ArrayList<>());
    }
    
    @Override
    public List<Instance> getAllInstances(String serviceName, boolean subscribe) throws NacosException {
        return getAllInstances(serviceName, new ArrayList<>(), subscribe);
    }
    
    @Override
    public List<Instance> getAllInstances(String serviceName, String groupName, boolean subscribe)
            throws NacosException {
        return getAllInstances(serviceName, groupName, new ArrayList<>(), subscribe);
    }
    
    @Override
    public List<Instance> getAllInstances(String serviceName, List<String> clusters) throws NacosException {
        return getAllInstances(serviceName, clusters, true);
    }
    
    @Override
    public List<Instance> getAllInstances(String serviceName, String groupName, List<String> clusters)
            throws NacosException {
        return getAllInstances(serviceName, groupName, clusters, true);
    }
    
    @Override
    public List<Instance> getAllInstances(String serviceName, List<String> clusters, boolean subscribe)
            throws NacosException {
        return getAllInstances(serviceName, Constants.DEFAULT_GROUP, clusters, subscribe);
    }
    
    @Override
    public List<Instance> getAllInstances(String serviceName, String groupName, List<String> clusters,
            boolean subscribe) throws NacosException {
        ServiceInfo serviceInfo;
        String clusterString = StringUtils.join(clusters, ",");
        // 是否订阅模式
        if (subscribe) {
            // 先从客户端缓存获取服务信息
            serviceInfo = serviceInfoHolder.getServiceInfo(serviceName, groupName, clusterString);
            if (null == serviceInfo || !clientProxy.isSubscribed(serviceName, groupName, clusterString)) {
                // 如果本地缓存不存在服务信息，则进行订阅
                serviceInfo = clientProxy.subscribe(serviceName, groupName, clusterString);
            }
        } else {
            // 如果未订阅服务信息，则直接去服务器进行查询
            serviceInfo = clientProxy.queryInstancesOfService(serviceName, groupName, clusterString, 0, false);
        }
        List<Instance> list;
        if (serviceInfo == null || CollectionUtils.isEmpty(list = serviceInfo.getHosts())) {
            return new ArrayList<>();
        }
        return list;
    }
    
    @Override
    public List<Instance> selectInstances(String serviceName, boolean healthy) throws NacosException {
        return selectInstances(serviceName, new ArrayList<>(), healthy);
    }
    
    @Override
    public List<Instance> selectInstances(String serviceName, String groupName, boolean healthy) throws NacosException {
        return selectInstances(serviceName, groupName, healthy, true);
    }
    
    @Override
    public List<Instance> selectInstances(String serviceName, boolean healthy, boolean subscribe)
            throws NacosException {
        return selectInstances(serviceName, new ArrayList<>(), healthy, subscribe);
    }
    
    @Override
    public List<Instance> selectInstances(String serviceName, String groupName, boolean healthy, boolean subscribe)
            throws NacosException {
        return selectInstances(serviceName, groupName, new ArrayList<>(), healthy, subscribe);
    }
    
    @Override
    public List<Instance> selectInstances(String serviceName, List<String> clusters, boolean healthy)
            throws NacosException {
        return selectInstances(serviceName, clusters, healthy, true);
    }
    
    @Override
    public List<Instance> selectInstances(String serviceName, String groupName, List<String> clusters, boolean healthy)
            throws NacosException {
        return selectInstances(serviceName, groupName, clusters, healthy, true);
    }
    
    @Override
    public List<Instance> selectInstances(String serviceName, List<String> clusters, boolean healthy, boolean subscribe)
            throws NacosException {
        return selectInstances(serviceName, Constants.DEFAULT_GROUP, clusters, healthy, subscribe);
    }
    
    @Override
    public List<Instance> selectInstances(String serviceName, String groupName, List<String> clusters, boolean healthy,
            boolean subscribe) throws NacosException {
        
        ServiceInfo serviceInfo;
        // 集群名称,使用逗号分隔
        String clusterString = StringUtils.join(clusters, ",");

        // 是否订阅，默认是订阅的
        if (subscribe) {
            /**
             * 1.从缓存中获取ServiceInfo
             * ConcurrentMap<String, ServiceInfo> serviceInfoMap
             * key:  groupName@@serviceName  或者  groupName@@serviceName@@clusterString
             * value: ServiceInfo
             */
            serviceInfo = serviceInfoHolder.getServiceInfo(serviceName, groupName, clusterString);
            // 2.缓存为空，执行订阅服务
            if (null == serviceInfo) {
                // clientProxy在构造方法中初始化为：NamingClientProxyDelegate
                serviceInfo = clientProxy.subscribe(serviceName, groupName, clusterString);
            }
        } else {
            // 3.非订阅，通过grpc发送ServiceQueryRequest服务查询请求
            // clientProxy在构造方法中初始化为：NamingClientProxyDelegate
            serviceInfo = clientProxy.queryInstancesOfService(serviceName, groupName, clusterString, 0, false);
        }
        // 4.筛选满足条件的实例
        return selectInstances(serviceInfo, healthy);
    }
    
    private List<Instance> selectInstances(ServiceInfo serviceInfo, boolean healthy) {
        List<Instance> list;
        if (serviceInfo == null || CollectionUtils.isEmpty(list = serviceInfo.getHosts())) {
            return new ArrayList<>();
        }

        // 遍历所有实例，直接移除掉不满足条件的实例
        Iterator<Instance> iterator = list.iterator();
        while (iterator.hasNext()) {
            Instance instance = iterator.next();
            // 筛选出健康、启用、权重大于0的实例
            if (healthy != instance.isHealthy() || !instance.isEnabled() || instance.getWeight() <= 0) {
                iterator.remove();
            }
        }
        
        return list;
    }
    
    @Override
    public Instance selectOneHealthyInstance(String serviceName) throws NacosException {
        return selectOneHealthyInstance(serviceName, new ArrayList<>());
    }
    
    @Override
    public Instance selectOneHealthyInstance(String serviceName, String groupName) throws NacosException {
        return selectOneHealthyInstance(serviceName, groupName, true);
    }
    
    @Override
    public Instance selectOneHealthyInstance(String serviceName, boolean subscribe) throws NacosException {
        return selectOneHealthyInstance(serviceName, new ArrayList<>(), subscribe);
    }
    
    @Override
    public Instance selectOneHealthyInstance(String serviceName, String groupName, boolean subscribe)
            throws NacosException {
        return selectOneHealthyInstance(serviceName, groupName, new ArrayList<>(), subscribe);
    }
    
    @Override
    public Instance selectOneHealthyInstance(String serviceName, List<String> clusters) throws NacosException {
        return selectOneHealthyInstance(serviceName, clusters, true);
    }
    
    @Override
    public Instance selectOneHealthyInstance(String serviceName, String groupName, List<String> clusters)
            throws NacosException {
        return selectOneHealthyInstance(serviceName, groupName, clusters, true);
    }
    
    @Override
    public Instance selectOneHealthyInstance(String serviceName, List<String> clusters, boolean subscribe)
            throws NacosException {
        return selectOneHealthyInstance(serviceName, Constants.DEFAULT_GROUP, clusters, subscribe);
    }
    
    @Override
    public Instance selectOneHealthyInstance(String serviceName, String groupName, List<String> clusters,
            boolean subscribe) throws NacosException {
        String clusterString = StringUtils.join(clusters, ",");
        // 订阅模式
        if (subscribe) {
            // 从本地缓存中获取ServiceInfo
            ServiceInfo serviceInfo = serviceInfoHolder.getServiceInfo(serviceName, groupName, clusterString);
            if (null == serviceInfo) {
                // 缓存为空，则订阅
                serviceInfo = clientProxy.subscribe(serviceName, groupName, clusterString);
            }
            // 通过负载均衡算法获得其中一个实例
            return Balancer.RandomByWeight.selectHost(serviceInfo);
        } else {
            // 通过grpc请求Nacos服务端查询
            ServiceInfo serviceInfo = clientProxy
                    .queryInstancesOfService(serviceName, groupName, clusterString, 0, false);
            // 通过负载均衡算法获得其中一个实例
            return Balancer.RandomByWeight.selectHost(serviceInfo);
        }
    }
    
    @Override
    public void subscribe(String serviceName, EventListener listener) throws NacosException {
        subscribe(serviceName, new ArrayList<>(), listener);
    }
    
    @Override
    public void subscribe(String serviceName, String groupName, EventListener listener) throws NacosException {
        subscribe(serviceName, groupName, new ArrayList<>(), listener);
    }
    
    @Override
    public void subscribe(String serviceName, List<String> clusters, EventListener listener) throws NacosException {
        subscribe(serviceName, Constants.DEFAULT_GROUP, clusters, listener);
    }
    
    @Override
    public void subscribe(String serviceName, String groupName, List<String> clusters, EventListener listener)
            throws NacosException {
        if (null == listener) {
            return;
        }
        String clusterString = StringUtils.join(clusters, ",");
        changeNotifier.registerListener(groupName, serviceName, clusterString, listener);
        clientProxy.subscribe(serviceName, groupName, clusterString);
    }
    
    @Override
    public void unsubscribe(String serviceName, EventListener listener) throws NacosException {
        unsubscribe(serviceName, new ArrayList<>(), listener);
    }
    
    @Override
    public void unsubscribe(String serviceName, String groupName, EventListener listener) throws NacosException {
        unsubscribe(serviceName, groupName, new ArrayList<>(), listener);
    }
    
    @Override
    public void unsubscribe(String serviceName, List<String> clusters, EventListener listener) throws NacosException {
        unsubscribe(serviceName, Constants.DEFAULT_GROUP, clusters, listener);
    }
    
    @Override
    public void unsubscribe(String serviceName, String groupName, List<String> clusters, EventListener listener)
            throws NacosException {
        String clustersString = StringUtils.join(clusters, ",");
        changeNotifier.deregisterListener(groupName, serviceName, clustersString, listener);
        if (!changeNotifier.isSubscribed(groupName, serviceName, clustersString)) {
            clientProxy.unsubscribe(serviceName, groupName, clustersString);
        }
    }
    
    @Override
    public ListView<String> getServicesOfServer(int pageNo, int pageSize) throws NacosException {
        return getServicesOfServer(pageNo, pageSize, Constants.DEFAULT_GROUP);
    }
    
    @Override
    public ListView<String> getServicesOfServer(int pageNo, int pageSize, String groupName) throws NacosException {
        return getServicesOfServer(pageNo, pageSize, groupName, null);
    }
    
    @Override
    public ListView<String> getServicesOfServer(int pageNo, int pageSize, AbstractSelector selector)
            throws NacosException {
        return getServicesOfServer(pageNo, pageSize, Constants.DEFAULT_GROUP, selector);
    }
    
    @Override
    public ListView<String> getServicesOfServer(int pageNo, int pageSize, String groupName, AbstractSelector selector)
            throws NacosException {
        return clientProxy.getServiceList(pageNo, pageSize, groupName, selector);
    }
    
    @Override
    public List<ServiceInfo> getSubscribeServices() {
        return changeNotifier.getSubscribeServices();
    }
    
    @Override
    public String getServerStatus() {
        return clientProxy.serverHealthy() ? UP : DOWN;
    }
    
    @Override
    public void shutDown() throws NacosException {
        serviceInfoHolder.shutdown();
        clientProxy.shutdown();
        NotifyCenter.deregisterSubscriber(changeNotifier);
    
    }
}
