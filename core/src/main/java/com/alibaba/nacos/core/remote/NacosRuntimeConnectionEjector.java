/*
 *
 * Copyright 1999-2021 Alibaba Group Holding Ltd.
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
 *
 */

package com.alibaba.nacos.core.remote;

import com.alibaba.nacos.api.remote.RequestCallBack;
import com.alibaba.nacos.api.remote.request.ClientDetectionRequest;
import com.alibaba.nacos.api.remote.response.Response;
import com.alibaba.nacos.common.remote.exception.ConnectionAlreadyClosedException;
import com.alibaba.nacos.common.utils.CollectionUtils;
import com.alibaba.nacos.core.monitor.MetricsMonitor;
import com.alibaba.nacos.plugin.control.Loggers;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

/**
 * nacos runtime connection ejector.
 *
 * @author shiyiyue
 */
public class NacosRuntimeConnectionEjector extends RuntimeConnectionEjector {
    
    public NacosRuntimeConnectionEjector() {
    
    }
    
    /**
     * eject connections on runtime.
     */
    public void doEject() {
        try {
    
            Loggers.CONNECTION.info("Connection check task start");

            // 拿到当前所有的连接
            Map<String, Connection> connections = connectionManager.connections;
            int totalCount = connections.size();
            MetricsMonitor.getLongConnectionMonitor().set(totalCount);
            int currentSdkClientCount = connectionManager.currentSdkClientCount();
            
            Loggers.CONNECTION.info("Long connection metrics detail ,Total count ={}, sdkCount={},clusterCount={}",
                    totalCount, currentSdkClientCount, (totalCount - currentSdkClientCount));

            // 用于存放超时的连接集合
            Set<String> outDatedConnections = new HashSet<>();
            long now = System.currentTimeMillis();
            for (Map.Entry<String, Connection> entry : connections.entrySet()) {
                Connection client = entry.getValue();
                // client.getMetaInfo().getLastActiveTime(): 客户端上一次活跃时间
                // 客户端上一次活跃时间距离当前时间超过20s的客户端，服务端会发起请求探活，如果失败或者超过指定时间未响应则剔除服务。
                if (now - client.getMetaInfo().getLastActiveTime() >= KEEP_ALIVE_TIME) {
                    outDatedConnections.add(client.getMetaInfo().getConnectionId());
                }
            }
            
            // check out date connection
            Loggers.CONNECTION.info("Out dated connection ,size={}", outDatedConnections.size());
            if (CollectionUtils.isNotEmpty(outDatedConnections)) {
                // 记录成功探活的客户端连接的集合
                Set<String> successConnections = new HashSet<>();
                final CountDownLatch latch = new CountDownLatch(outDatedConnections.size());
                for (String outDateConnectionId : outDatedConnections) {
                    try {
                        Connection connection = connectionManager.getConnection(outDateConnectionId);
                        if (connection != null) {
                            // 创建一个客户端检测请求
                            ClientDetectionRequest clientDetectionRequest = new ClientDetectionRequest();
                            connection.asyncRequest(clientDetectionRequest, new RequestCallBack() {
                                @Override
                                public Executor getExecutor() {
                                    return null;
                                }
                                
                                @Override
                                public long getTimeout() {
                                    return 5000L;
                                }
                                
                                @Override
                                public void onResponse(Response response) {
                                    latch.countDown();
                                    if (response != null && response.isSuccess()) {
                                        // 探活成功，更新活跃时间，然后加入到探活成功的集合中
                                        connection.freshActiveTime();
                                        successConnections.add(outDateConnectionId);
                                    }
                                }
                                
                                @Override
                                public void onException(Throwable e) {
                                    latch.countDown();
                                }
                            });
                            
                            Loggers.CONNECTION.info("[{}]send connection active request ", outDateConnectionId);
                        } else {
                            latch.countDown();
                        }
                        
                    } catch (ConnectionAlreadyClosedException e) {
                        latch.countDown();
                    } catch (Exception e) {
                        Loggers.CONNECTION.error("[{}]Error occurs when check client active detection ,error={}",
                                outDateConnectionId, e);
                        latch.countDown();
                    }
                }
                
                latch.await(5000L, TimeUnit.MILLISECONDS);
                Loggers.CONNECTION.info("Out dated connection check successCount={}", successConnections.size());
                
                for (String outDateConnectionId : outDatedConnections) {
                    // 不在探活成功的集合，说明探活失败，执行注销连接操作
                    if (!successConnections.contains(outDateConnectionId)) {
                        Loggers.CONNECTION.info("[{}]Unregister Out dated connection....", outDateConnectionId);
                        // 注销过期连接
                        connectionManager.unregister(outDateConnectionId);
                    }
                }
            }
            
            Loggers.CONNECTION.info("Connection check task end");
            
        } catch (Throwable e) {
            Loggers.CONNECTION.error("Error occurs during connection check... ", e);
        }
    }
    
    @Override
    public String getName() {
        return "nacos";
    }
}
