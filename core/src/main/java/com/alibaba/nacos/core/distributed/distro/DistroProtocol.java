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

package com.alibaba.nacos.core.distributed.distro;

import com.alibaba.nacos.consistency.DataOperation;
import com.alibaba.nacos.core.cluster.Member;
import com.alibaba.nacos.core.cluster.ServerMemberManager;
import com.alibaba.nacos.core.distributed.distro.component.DistroCallback;
import com.alibaba.nacos.core.distributed.distro.component.DistroComponentHolder;
import com.alibaba.nacos.core.distributed.distro.component.DistroDataProcessor;
import com.alibaba.nacos.core.distributed.distro.component.DistroDataStorage;
import com.alibaba.nacos.core.distributed.distro.component.DistroTransportAgent;
import com.alibaba.nacos.core.distributed.distro.entity.DistroData;
import com.alibaba.nacos.core.distributed.distro.entity.DistroKey;
import com.alibaba.nacos.core.distributed.distro.task.DistroTaskEngineHolder;
import com.alibaba.nacos.core.distributed.distro.task.delay.DistroDelayTask;
import com.alibaba.nacos.core.distributed.distro.task.load.DistroLoadDataTask;
import com.alibaba.nacos.core.distributed.distro.task.verify.DistroVerifyTimedTask;
import com.alibaba.nacos.core.utils.GlobalExecutor;
import com.alibaba.nacos.core.utils.Loggers;
import com.alibaba.nacos.sys.env.EnvUtil;
import org.springframework.stereotype.Component;

/**
 * Distro protocol.
 *
 * @author xiweng.yy
 */
@Component
public class DistroProtocol {

    /**
     * 集群成员的管理类
     */
    private final ServerMemberManager memberManager;
    
    private final DistroComponentHolder distroComponentHolder;
    
    private final DistroTaskEngineHolder distroTaskEngineHolder;
    
    private volatile boolean isInitialized = false;
    
    public DistroProtocol(ServerMemberManager memberManager, DistroComponentHolder distroComponentHolder,
            DistroTaskEngineHolder distroTaskEngineHolder) {
        this.memberManager = memberManager;
        this.distroComponentHolder = distroComponentHolder;
        this.distroTaskEngineHolder = distroTaskEngineHolder;
        // 在服务启动时会去其它成员服务中同步注册的实例数据
        startDistroTask();
    }
    
    private void startDistroTask() {
        // 只有集群模式才会执行同步
        if (EnvUtil.getStandaloneMode()) {
            isInitialized = true;
            return;
        }
        // 注册的客户端服务校验5s后开始，每5s执行一次
        startVerifyTask();
        // 注册的客户端服务数据同步
        startLoadTask();
    }
    
    private void startLoadTask() {
        DistroCallback loadCallback = new DistroCallback() {
            @Override
            public void onSuccess() {
                isInitialized = true;
            }
            
            @Override
            public void onFailed(Throwable throwable) {
                isInitialized = false;
            }
        };
        GlobalExecutor.submitLoadDataTask(
                new DistroLoadDataTask(memberManager, distroComponentHolder, DistroConfig.getInstance(), loadCallback));
    }
    
    private void startVerifyTask() {
        GlobalExecutor.schedulePartitionDataTimedSync(new DistroVerifyTimedTask(memberManager, distroComponentHolder,
                        distroTaskEngineHolder.getExecuteWorkersManager()),
                DistroConfig.getInstance().getVerifyIntervalMillis());
    }
    
    public boolean isInitialized() {
        return isInitialized;
    }
    
    /**
     * Start to sync by configured delay.
     *
     * @param distroKey distro key of sync data
     * @param action    the action of data operation
     */
    public void sync(DistroKey distroKey, DataOperation action) {
        // 配置同步延迟的时间：默认为1s
        sync(distroKey, action, DistroConfig.getInstance().getSyncDelayMillis());
    }
    
    /**
     * Start to sync data to all remote server.
     *
     * @param distroKey distro key of sync data
     * @param action    the action of data operation
     * @param delay     delay time for sync
     */
    public void sync(DistroKey distroKey, DataOperation action, long delay) {
        // 遍历每个除自己以外的其它成员
        for (Member each : memberManager.allMembersWithoutSelf()) {
            syncToTarget(distroKey, action, each.getAddress(), delay);
        }
    }
    
    /**
     * Start to sync to target server.
     *
     * @param distroKey    distro key of sync data
     * @param action       the action of data operation
     * @param targetServer target server
     * @param delay        delay time for sync
     */
    public void syncToTarget(DistroKey distroKey, DataOperation action, String targetServer, long delay) {
        // 重新封装DistroKey，将目标节点的地址也封装进去
        DistroKey distroKeyWithTarget = new DistroKey(distroKey.getResourceKey(), distroKey.getResourceType(),
                targetServer);
        // 包装成一个延时任务，交给延迟任务引擎来处理
        DistroDelayTask distroDelayTask = new DistroDelayTask(distroKeyWithTarget, action, delay);
        // 往延时任务引擎中加入DistroDelayTask任务,最终将会调用DistroDelayTaskProcessor.process方法
        // distroTaskEngineHolder.getDelayTaskExecuteEngine()返回的是DistroDelayTaskExecuteEngine，它继承自NacosDelayTaskExecuteEngine,
        // 其构造方法中初始化了一个定时任务ProcessRunnable，不断从阻塞队列中拿出任务出来执行（ConcurrentHashMap<Object, AbstractDelayTask> tasks）
        distroTaskEngineHolder.getDelayTaskExecuteEngine().addTask(distroKeyWithTarget, distroDelayTask);
        if (Loggers.DISTRO.isDebugEnabled()) {
            Loggers.DISTRO.debug("[DISTRO-SCHEDULE] {} to {}", distroKey, targetServer);
        }
    }
    
    /**
     * Query data from specified server.
     *
     * @param distroKey data key
     * @return data
     */
    public DistroData queryFromRemote(DistroKey distroKey) {
        if (null == distroKey.getTargetServer()) {
            Loggers.DISTRO.warn("[DISTRO] Can't query data from empty server");
            return null;
        }
        String resourceType = distroKey.getResourceType();
        DistroTransportAgent transportAgent = distroComponentHolder.findTransportAgent(resourceType);
        if (null == transportAgent) {
            Loggers.DISTRO.warn("[DISTRO] Can't find transport agent for key {}", resourceType);
            return null;
        }
        return transportAgent.getData(distroKey, distroKey.getTargetServer());
    }
    
    /**
     * Receive synced distro data, find processor to process.
     *
     * @param distroData Received data
     * @return true if handle receive data successfully, otherwise false
     */
    public boolean onReceive(DistroData distroData) {
        Loggers.DISTRO.info("[DISTRO] Receive distro data type: {}, key: {}", distroData.getType(),
                distroData.getDistroKey());
        String resourceType = distroData.getDistroKey().getResourceType();
        DistroDataProcessor dataProcessor = distroComponentHolder.findDataProcessor(resourceType);
        if (null == dataProcessor) {
            Loggers.DISTRO.warn("[DISTRO] Can't find data process for received data {}", resourceType);
            return false;
        }
        // 通过处理器处理接收到的数据
        return dataProcessor.processData(distroData);
    }
    
    /**
     * Receive verify data, find processor to process.
     *
     * @param distroData    verify data
     * @param sourceAddress source server address, might be get data from source server
     * @return true if verify data successfully, otherwise false
     */
    public boolean onVerify(DistroData distroData, String sourceAddress) {
        if (Loggers.DISTRO.isDebugEnabled()) {
            Loggers.DISTRO.debug("[DISTRO] Receive verify data type: {}, key: {}", distroData.getType(),
                    distroData.getDistroKey());
        }
        String resourceType = distroData.getDistroKey().getResourceType();
        DistroDataProcessor dataProcessor = distroComponentHolder.findDataProcessor(resourceType);
        if (null == dataProcessor) {
            Loggers.DISTRO.warn("[DISTRO] Can't find verify data process for received data {}", resourceType);
            return false;
        }
        return dataProcessor.processVerifyData(distroData, sourceAddress);
    }
    
    /**
     * Query data of input distro key.
     *
     * @param distroKey key of data
     * @return data
     */
    public DistroData onQuery(DistroKey distroKey) {
        String resourceType = distroKey.getResourceType();
        DistroDataStorage distroDataStorage = distroComponentHolder.findDataStorage(resourceType);
        if (null == distroDataStorage) {
            Loggers.DISTRO.warn("[DISTRO] Can't find data storage for received key {}", resourceType);
            return new DistroData(distroKey, new byte[0]);
        }
        return distroDataStorage.getDistroData(distroKey);
    }
    
    /**
     * Query all datum snapshot.
     *
     * @param type datum type
     * @return all datum snapshot
     */
    public DistroData onSnapshot(String type) {
        DistroDataStorage distroDataStorage = distroComponentHolder.findDataStorage(type);
        if (null == distroDataStorage) {
            Loggers.DISTRO.warn("[DISTRO] Can't find data storage for received key {}", type);
            return new DistroData(new DistroKey("snapshot", type), new byte[0]);
        }
        return distroDataStorage.getDatumSnapshot();
    }
}
