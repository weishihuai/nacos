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

package com.alibaba.nacos.naming.controllers;

import com.alibaba.nacos.api.common.Constants;
import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.naming.CommonParams;
import com.alibaba.nacos.api.naming.pojo.Instance;
import com.alibaba.nacos.api.naming.utils.NamingUtils;
import com.alibaba.nacos.auth.annotation.Secured;
import com.alibaba.nacos.common.notify.NotifyCenter;
import com.alibaba.nacos.common.spi.NacosServiceLoader;
import com.alibaba.nacos.common.trace.DeregisterInstanceReason;
import com.alibaba.nacos.common.trace.event.naming.DeregisterInstanceTraceEvent;
import com.alibaba.nacos.common.trace.event.naming.RegisterInstanceTraceEvent;
import com.alibaba.nacos.common.utils.ConvertUtils;
import com.alibaba.nacos.common.utils.JacksonUtils;
import com.alibaba.nacos.common.utils.StringUtils;
import com.alibaba.nacos.core.utils.WebUtils;
import com.alibaba.nacos.naming.core.InstanceOperator;
import com.alibaba.nacos.naming.core.InstanceOperatorClientImpl;
import com.alibaba.nacos.naming.core.InstancePatchObject;
import com.alibaba.nacos.naming.healthcheck.RsInfo;
import com.alibaba.nacos.naming.misc.Loggers;
import com.alibaba.nacos.naming.misc.SwitchDomain;
import com.alibaba.nacos.naming.misc.SwitchEntry;
import com.alibaba.nacos.naming.misc.UtilsAndCommons;
import com.alibaba.nacos.naming.pojo.InstanceOperationInfo;
import com.alibaba.nacos.naming.pojo.Subscriber;
import com.alibaba.nacos.naming.pojo.instance.BeatInfoInstanceBuilder;
import com.alibaba.nacos.naming.pojo.instance.HttpRequestInstanceBuilder;
import com.alibaba.nacos.naming.pojo.instance.InstanceExtensionHandler;
import com.alibaba.nacos.naming.web.CanDistro;
import com.alibaba.nacos.plugin.auth.constant.ActionTypes;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.collections.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.alibaba.nacos.naming.misc.UtilsAndCommons.DEFAULT_CLUSTER_NAME;

/**
 * Instance operation controller.
 *
 * @author nkorange
 */
@RestController
@RequestMapping(UtilsAndCommons.NACOS_NAMING_CONTEXT + UtilsAndCommons.NACOS_NAMING_INSTANCE_CONTEXT)
public class InstanceController {
    
    @Autowired
    private SwitchDomain switchDomain;
    
    @Autowired
    private InstanceOperatorClientImpl instanceServiceV2;
    
    private static final String METADATA = "metadata";
    
    public InstanceController() {
        Collection<InstanceExtensionHandler> handlers = NacosServiceLoader.load(InstanceExtensionHandler.class);
        Loggers.SRV_LOG.info("Load instance extension handler {}", handlers);
    }
    
    /**
     * Nacos Server接收服务注册请求的入口（http方式）
     *
     * Nacos服务端注册服务实例
     * Register new instance.
     *
     * @param request http request
     * @return 'ok' if success 如果成功的话，返回ok
     * @throws Exception any error during register
     */
    @CanDistro
    @PostMapping
    @Secured(action = ActionTypes.WRITE)
    public String register(HttpServletRequest request) throws Exception {
        // 获取命名空间ID，实际上就是从请求参数中获取namespaceId的值.  本例中namespaceId的值是public
        final String namespaceId = WebUtils
                .optional(request, CommonParams.NAMESPACE_ID, Constants.DEFAULT_NAMESPACE_ID);
        // 获取带组名的服务名称，实际上就是从请求参数中获取serviceName的值。 本例中serviceName的值是DEFAULT_GROUP@@discovery-provider
        final String serviceName = WebUtils.required(request, CommonParams.SERVICE_NAME);
        // 检查服务名称是否合法
        NamingUtils.checkServiceNameFormat(serviceName);

        // 根据客户端传递的参数构建Instance服务实例
        final Instance instance = HttpRequestInstanceBuilder.newBuilder()
                .setDefaultInstanceEphemeral(switchDomain.isDefaultInstanceEphemeral()).setRequest(request).build();

        // 真正注册服务的地方，serviceName为注册的服务名，namespaceId默认为public
        // namespaceId可以做一层数据隔离
        getInstanceOperator().registerInstance(namespaceId, serviceName, instance);

        // 发布事件
        NotifyCenter.publishEvent(new RegisterInstanceTraceEvent(System.currentTimeMillis(), "", false, namespaceId,
                NamingUtils.getGroupName(serviceName), NamingUtils.getServiceName(serviceName), instance.getIp(),
                instance.getPort()));
        return "ok";
    }
    
    /**
     * /
     * 服务端处理下线服务实例
     * Deregister instances.
     *
     * @param request http request
     * @return 'ok' if success
     * @throws Exception any error during deregister
     */
    @CanDistro
    @DeleteMapping
    @Secured(action = ActionTypes.WRITE)
    public String deregister(HttpServletRequest request) throws Exception {
        // 根据客户端传递的参数构建Instance服务实例
        Instance instance = HttpRequestInstanceBuilder.newBuilder()
                .setDefaultInstanceEphemeral(switchDomain.isDefaultInstanceEphemeral()).setRequest(request).build();
        // 获取命名空间ID。 默认为public
        String namespaceId = WebUtils.optional(request, CommonParams.NAMESPACE_ID, Constants.DEFAULT_NAMESPACE_ID);
        // 获取服务名称
        String serviceName = WebUtils.required(request, CommonParams.SERVICE_NAME);
        // 检查服务名称是否合法
        NamingUtils.checkServiceNameFormat(serviceName);
        // 移除服务实例
        getInstanceOperator().removeInstance(namespaceId, serviceName, instance);
        // 发布实例下线事件
        NotifyCenter.publishEvent(new DeregisterInstanceTraceEvent(System.currentTimeMillis(), "", false,
                DeregisterInstanceReason.REQUEST, namespaceId, NamingUtils.getGroupName(serviceName),
                NamingUtils.getServiceName(serviceName), instance.getIp(), instance.getPort()));
        return "ok";
    }
    
    /**
     * Update instance.
     *
     * @param request http request
     * @return 'ok' if success
     * @throws Exception any error during update
     */
    @CanDistro
    @PutMapping
    @Secured(action = ActionTypes.WRITE)
    public String update(HttpServletRequest request) throws Exception {
        String namespaceId = WebUtils.optional(request, CommonParams.NAMESPACE_ID, Constants.DEFAULT_NAMESPACE_ID);
        String serviceName = WebUtils.required(request, CommonParams.SERVICE_NAME);
        NamingUtils.checkServiceNameFormat(serviceName);
        Instance instance = HttpRequestInstanceBuilder.newBuilder()
                .setDefaultInstanceEphemeral(switchDomain.isDefaultInstanceEphemeral()).setRequest(request).build();
        getInstanceOperator().updateInstance(namespaceId, serviceName, instance);
        return "ok";
    }
    
    /**
     * Batch update instance's metadata. old key exist = update, old key not exist = add.
     *
     * @param request http request
     * @return success updated instances. such as '{"updated":["2.2.2.2:8080:unknown:xxxx-cluster:ephemeral"}'.
     * @throws Exception any error during update
     * @since 1.4.0
     */
    @CanDistro
    @PutMapping(value = "/metadata/batch")
    @Secured(action = ActionTypes.WRITE)
    public ObjectNode batchUpdateInstanceMetadata(HttpServletRequest request) throws Exception {
        final String namespaceId = WebUtils
                .optional(request, CommonParams.NAMESPACE_ID, Constants.DEFAULT_NAMESPACE_ID);
        String serviceName = WebUtils.required(request, CommonParams.SERVICE_NAME);
        String consistencyType = WebUtils.optional(request, "consistencyType", StringUtils.EMPTY);
        String instances = WebUtils.optional(request, "instances", StringUtils.EMPTY);
        List<Instance> targetInstances = parseBatchInstances(instances);
        String metadata = WebUtils.required(request, METADATA);
        Map<String, String> targetMetadata = UtilsAndCommons.parseMetadata(metadata);
        InstanceOperationInfo instanceOperationInfo = buildOperationInfo(serviceName, consistencyType, targetInstances);
        
        List<String> operatedInstances = getInstanceOperator()
                .batchUpdateMetadata(namespaceId, instanceOperationInfo, targetMetadata);
        ObjectNode result = JacksonUtils.createEmptyJsonNode();
        ArrayNode ipArray = JacksonUtils.createEmptyArrayNode();
        for (String ip : operatedInstances) {
            ipArray.add(ip);
        }
        result.replace("updated", ipArray);
        return result;
    }
    
    /**
     * Batch delete instance's metadata. old key exist = delete, old key not exist = not operate
     *
     * @param request http request
     * @return success updated instances. such as '{"updated":["2.2.2.2:8080:unknown:xxxx-cluster:ephemeral"}'.
     * @throws Exception any error during update
     * @since 1.4.0
     */
    @CanDistro
    @DeleteMapping("/metadata/batch")
    @Secured(action = ActionTypes.WRITE)
    public ObjectNode batchDeleteInstanceMetadata(HttpServletRequest request) throws Exception {
        final String namespaceId = WebUtils
                .optional(request, CommonParams.NAMESPACE_ID, Constants.DEFAULT_NAMESPACE_ID);
        String serviceName = WebUtils.required(request, CommonParams.SERVICE_NAME);
        String consistencyType = WebUtils.optional(request, "consistencyType", StringUtils.EMPTY);
        String instances = WebUtils.optional(request, "instances", StringUtils.EMPTY);
        List<Instance> targetInstances = parseBatchInstances(instances);
        String metadata = WebUtils.required(request, METADATA);
        Map<String, String> targetMetadata = UtilsAndCommons.parseMetadata(metadata);
        InstanceOperationInfo instanceOperationInfo = buildOperationInfo(serviceName, consistencyType, targetInstances);
        List<String> operatedInstances = getInstanceOperator()
                .batchDeleteMetadata(namespaceId, instanceOperationInfo, targetMetadata);
        
        ObjectNode result = JacksonUtils.createEmptyJsonNode();
        ArrayNode ipArray = JacksonUtils.createEmptyArrayNode();
        for (String ip : operatedInstances) {
            ipArray.add(ip);
        }
        result.replace("updated", ipArray);
        return result;
    }
    
    private InstanceOperationInfo buildOperationInfo(String serviceName, String consistencyType,
            List<Instance> instances) {
        if (!CollectionUtils.isEmpty(instances)) {
            for (Instance instance : instances) {
                if (StringUtils.isBlank(instance.getClusterName())) {
                    instance.setClusterName(DEFAULT_CLUSTER_NAME);
                }
            }
        }
        return new InstanceOperationInfo(serviceName, consistencyType, instances);
    }
    
    private List<Instance> parseBatchInstances(String instances) {
        try {
            return JacksonUtils.toObj(instances, new TypeReference<List<Instance>>() {
            });
        } catch (Exception e) {
            Loggers.SRV_LOG.warn("UPDATE-METADATA: Param 'instances' is illegal, ignore this operation", e);
        }
        return Collections.emptyList();
    }
    
    
    /**
     * Patch instance.
     *
     * @param request http request
     * @return 'ok' if success
     * @throws Exception any error during patch
     */
    @CanDistro
    @PatchMapping
    @Secured(action = ActionTypes.WRITE)
    public String patch(HttpServletRequest request) throws Exception {
        String serviceName = WebUtils.required(request, CommonParams.SERVICE_NAME);
        NamingUtils.checkServiceNameFormat(serviceName);
        String ip = WebUtils.required(request, "ip");
        String port = WebUtils.required(request, "port");
        String cluster = WebUtils.optional(request, CommonParams.CLUSTER_NAME, StringUtils.EMPTY);
        if (StringUtils.isBlank(cluster)) {
            cluster = WebUtils.optional(request, "cluster", UtilsAndCommons.DEFAULT_CLUSTER_NAME);
        }
        InstancePatchObject patchObject = new InstancePatchObject(cluster, ip, Integer.parseInt(port));
        String metadata = WebUtils.optional(request, METADATA, StringUtils.EMPTY);
        if (StringUtils.isNotBlank(metadata)) {
            patchObject.setMetadata(UtilsAndCommons.parseMetadata(metadata));
        }
        String weight = WebUtils.optional(request, "weight", StringUtils.EMPTY);
        if (StringUtils.isNotBlank(weight)) {
            patchObject.setWeight(Double.parseDouble(weight));
        }
        String healthy = WebUtils.optional(request, "healthy", StringUtils.EMPTY);
        if (StringUtils.isNotBlank(healthy)) {
            patchObject.setHealthy(ConvertUtils.toBoolean(healthy));
        }
        String enabledString = WebUtils.optional(request, "enabled", StringUtils.EMPTY);
        if (StringUtils.isNotBlank(enabledString)) {
            patchObject.setEnabled(ConvertUtils.toBoolean(enabledString));
        }
        String namespaceId = WebUtils.optional(request, CommonParams.NAMESPACE_ID, Constants.DEFAULT_NAMESPACE_ID);
        getInstanceOperator().patchInstance(namespaceId, serviceName, patchObject);
        return "ok";
    }
    
    /**
     * Get all instance of input service.
     *
     * @param request http request
     * @return list of instance
     * @throws Exception any error during list
     */
    @GetMapping("/list")
    @Secured(action = ActionTypes.READ)
    public Object list(HttpServletRequest request) throws Exception {
        
        String namespaceId = WebUtils.optional(request, CommonParams.NAMESPACE_ID, Constants.DEFAULT_NAMESPACE_ID);
        String serviceName = WebUtils.required(request, CommonParams.SERVICE_NAME);
        NamingUtils.checkServiceNameFormat(serviceName);
        
        String agent = WebUtils.getUserAgent(request);
        String clusters = WebUtils.optional(request, "clusters", StringUtils.EMPTY);
        String clientIP = WebUtils.optional(request, "clientIP", StringUtils.EMPTY);
        int udpPort = Integer.parseInt(WebUtils.optional(request, "udpPort", "0"));
        boolean healthyOnly = Boolean.parseBoolean(WebUtils.optional(request, "healthyOnly", "false"));
        String app = WebUtils.optional(request, "app", StringUtils.EMPTY);
        
        Subscriber subscriber = new Subscriber(clientIP + ":" + udpPort, agent, app, clientIP, namespaceId, serviceName,
                udpPort, clusters);
        return getInstanceOperator().listInstance(namespaceId, serviceName, subscriber, clusters, healthyOnly);
    }
    
    /**
     * Get detail information of specified instance.
     *
     * @param request http request
     * @return detail information of instance
     * @throws Exception any error during get
     */
    @GetMapping
    @Secured(action = ActionTypes.READ)
    public ObjectNode detail(HttpServletRequest request) throws Exception {
        
        String namespaceId = WebUtils.optional(request, CommonParams.NAMESPACE_ID, Constants.DEFAULT_NAMESPACE_ID);
        String serviceName = WebUtils.required(request, CommonParams.SERVICE_NAME);
        NamingUtils.checkServiceNameFormat(serviceName);
        String cluster = WebUtils.optional(request, CommonParams.CLUSTER_NAME, UtilsAndCommons.DEFAULT_CLUSTER_NAME);
        String ip = WebUtils.required(request, "ip");
        int port = Integer.parseInt(WebUtils.required(request, "port"));
        
        com.alibaba.nacos.api.naming.pojo.Instance instance = getInstanceOperator()
                .getInstance(namespaceId, serviceName, cluster, ip, port);
        ObjectNode result = JacksonUtils.createEmptyJsonNode();
        result.put("service", serviceName);
        result.put("ip", ip);
        result.put("port", port);
        result.put("clusterName", cluster);
        result.put("weight", instance.getWeight());
        result.put("healthy", instance.isHealthy());
        result.put("instanceId", instance.getInstanceId());
        result.set(METADATA, JacksonUtils.transferToJsonNode(instance.getMetadata()));
        return result;
    }
    
    /**
     * 服务端处理客户端定时发送的心跳信息（服务续约）
     *
     * Create a beat for instance.
     *
     * @param request http request
     * @return detail information of instance
     * @throws Exception any error during handle
     */
    @CanDistro
    @PutMapping("/beat")
    @Secured(action = ActionTypes.WRITE)
    public ObjectNode beat(HttpServletRequest request) throws Exception {
        
        ObjectNode result = JacksonUtils.createEmptyJsonNode();
        // 设置客户端下一次的心跳间隔时间，默认5s
        // private long clientBeatInterval = TimeUnit.SECONDS.toMillis(5);
        result.put(SwitchEntry.CLIENT_BEAT_INTERVAL, switchDomain.getClientBeatInterval());
        // 从请求中解析的心跳信息
        String beat = WebUtils.optional(request, "beat", StringUtils.EMPTY);
        RsInfo clientBeat = null;
        // 如果beat数据不等于空，判断是否是注册心跳
        if (StringUtils.isNotBlank(beat)) {
            // 字符串转对象
            clientBeat = JacksonUtils.toObj(beat, RsInfo.class);
        }
        // 获取集群名称（default）
        String clusterName = WebUtils
                .optional(request, CommonParams.CLUSTER_NAME, UtilsAndCommons.DEFAULT_CLUSTER_NAME);
        // 获取IP
        String ip = WebUtils.optional(request, "ip", StringUtils.EMPTY);
        // 获取端口
        int port = Integer.parseInt(WebUtils.optional(request, "port", "0"));
        if (clientBeat != null) {
            if (StringUtils.isNotBlank(clientBeat.getCluster())) {
                clusterName = clientBeat.getCluster();
            } else {
                // fix #2533
                clientBeat.setCluster(clusterName);
            }
            ip = clientBeat.getIp();
            port = clientBeat.getPort();
        }
        // 获取命名空间 （public）
        String namespaceId = WebUtils.optional(request, CommonParams.NAMESPACE_ID, Constants.DEFAULT_NAMESPACE_ID);
        // 获取服务名称
        String serviceName = WebUtils.required(request, CommonParams.SERVICE_NAME);
        // 校验服务名是否合法
        NamingUtils.checkServiceNameFormat(serviceName);
        Loggers.SRV_LOG.debug("[CLIENT-BEAT] full arguments: beat: {}, serviceName: {}, namespaceId: {}", clientBeat,
                serviceName, namespaceId);
        BeatInfoInstanceBuilder builder = BeatInfoInstanceBuilder.newBuilder();
        builder.setRequest(request);
        // 处理心跳信息，并组装结果进行返回
        int resultCode = getInstanceOperator()
                .handleBeat(namespaceId, serviceName, ip, port, clusterName, clientBeat, builder);
        // 设置心跳处理结果code
        result.put(CommonParams.CODE, resultCode);
        // 设置客户端下一次心跳间隔周期
        result.put(SwitchEntry.CLIENT_BEAT_INTERVAL,
                getInstanceOperator().getHeartBeatInterval(namespaceId, serviceName, ip, port, clusterName));
        // 设置是否轻量级心跳的为true,那么下一次心跳请求的时候，客户端，就不会携带beat数据
        result.put(SwitchEntry.LIGHT_BEAT_ENABLED, switchDomain.isLightBeatEnabled());
        return result;
    }
    
    /**
     * List all instance with health status.
     *
     * @param key (namespace##)?serviceName
     * @return list of instance
     * @throws NacosException any error during handle
     */
    @RequestMapping("/statuses")
    public ObjectNode listWithHealthStatus(@RequestParam String key) throws NacosException {
        
        String serviceName;
        String namespaceId;
        
        if (key.contains(UtilsAndCommons.NAMESPACE_SERVICE_CONNECTOR)) {
            namespaceId = key.split(UtilsAndCommons.NAMESPACE_SERVICE_CONNECTOR)[0];
            serviceName = key.split(UtilsAndCommons.NAMESPACE_SERVICE_CONNECTOR)[1];
        } else {
            namespaceId = Constants.DEFAULT_NAMESPACE_ID;
            serviceName = key;
        }
        NamingUtils.checkServiceNameFormat(serviceName);
        
        List<? extends com.alibaba.nacos.api.naming.pojo.Instance> ips = getInstanceOperator()
                .listAllInstances(namespaceId, serviceName);
        
        ObjectNode result = JacksonUtils.createEmptyJsonNode();
        ArrayNode ipArray = JacksonUtils.createEmptyArrayNode();
        for (com.alibaba.nacos.api.naming.pojo.Instance ip : ips) {
            ipArray.add(ip.toInetAddr() + "_" + ip.isHealthy());
        }
        result.replace("ips", ipArray);
        return result;
    }
    
    private InstanceOperator getInstanceOperator() {
        return instanceServiceV2;
    }
}
