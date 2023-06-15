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

package com.alibaba.nacos.api.naming.pojo.builder;

import com.alibaba.nacos.api.naming.pojo.Instance;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Builder for {@link Instance}.
 *
 * @author xiweng.yy
 */
public class InstanceBuilder {
    
    private String instanceId;
    
    private String ip;
    
    private Integer port;
    
    private Double weight;
    
    private Boolean healthy;
    
    private Boolean enabled;
    
    private Boolean ephemeral;
    
    private String clusterName;
    
    private String serviceName;
    
    private Map<String, String> metadata = new HashMap<>();
    
    private InstanceBuilder() {
    }
    
    public InstanceBuilder setInstanceId(String instanceId) {
        this.instanceId = instanceId;
        return this;
    }
    
    public InstanceBuilder setIp(String ip) {
        this.ip = ip;
        return this;
    }
    
    public InstanceBuilder setPort(Integer port) {
        this.port = port;
        return this;
    }
    
    public InstanceBuilder setWeight(Double weight) {
        this.weight = weight;
        return this;
    }
    
    public InstanceBuilder setHealthy(Boolean healthy) {
        this.healthy = healthy;
        return this;
    }
    
    public InstanceBuilder setEnabled(Boolean enabled) {
        this.enabled = enabled;
        return this;
    }
    
    public InstanceBuilder setEphemeral(Boolean ephemeral) {
        this.ephemeral = ephemeral;
        return this;
    }
    
    public InstanceBuilder setClusterName(String clusterName) {
        this.clusterName = clusterName;
        return this;
    }
    
    public InstanceBuilder setServiceName(String serviceName) {
        this.serviceName = serviceName;
        return this;
    }
    
    public InstanceBuilder setMetadata(Map<String, String> metadata) {
        this.metadata = metadata;
        return this;
    }
    
    public InstanceBuilder addMetadata(String metaKey, String metaValue) {
        this.metadata.put(metaKey, metaValue);
        return this;
    }
    
    /**
     * 构建一个新的服务实例
     * Build a new {@link Instance}.
     *
     * instanceId：实例的唯一ID;
     * ip：实例IP，提供给消费者进行通信的地址;
     * port：端口，提供给消费者访问的端口;
     * weight：权重，当前实例的权限，浮点类型(默认1.0D);
     * healthy：健康状况，默认true;
     * enabled：实例是否准备好接收请求，默认true;
     * ephemeral：实例是否为瞬时的，默认为true;
     * clusterName：实例所属的集群名称;
     * serviceName：实例的服务信息;
     *
     * @return new instance
     */
    public Instance build() {
        Instance result = new Instance();
        if (!Objects.isNull(instanceId)) {
            result.setInstanceId(instanceId);
        }
        // 设置IP地址
        if (!Objects.isNull(ip)) {
            result.setIp(ip);
        }
        // 设置服务端口
        if (!Objects.isNull(port)) {
            result.setPort(port);
        }
        // 设置权重
        if (!Objects.isNull(weight)) {
            result.setWeight(weight);
        }
        // 设置实例健康状态
        if (!Objects.isNull(healthy)) {
            result.setHealthy(healthy);
        }
        // 设置是否启用
        if (!Objects.isNull(enabled)) {
            result.setEnabled(enabled);
        }
        // 设置是否临时实例
        if (!Objects.isNull(ephemeral)) {
            result.setEphemeral(ephemeral);
        }
        // 设置集群名称
        if (!Objects.isNull(clusterName)) {
            result.setClusterName(clusterName);
        }
        // 设置服务名称
        if (!Objects.isNull(serviceName)) {
            result.setServiceName(serviceName);
        }
        // 设置实例名称
        result.setMetadata(metadata);
        return result;
    }
    
    public static InstanceBuilder newBuilder() {
        return new InstanceBuilder();
    }
}
