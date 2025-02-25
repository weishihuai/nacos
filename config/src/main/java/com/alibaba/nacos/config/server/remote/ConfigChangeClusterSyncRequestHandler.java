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

package com.alibaba.nacos.config.server.remote;

import com.alibaba.nacos.api.config.remote.request.cluster.ConfigChangeClusterSyncRequest;
import com.alibaba.nacos.api.config.remote.response.cluster.ConfigChangeClusterSyncResponse;
import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.remote.request.RequestMeta;
import com.alibaba.nacos.common.utils.StringUtils;
import com.alibaba.nacos.config.server.service.dump.DumpService;
import com.alibaba.nacos.core.remote.RequestHandler;
import com.alibaba.nacos.core.control.TpsControl;
import org.springframework.stereotype.Component;

/**
 * handller to handler config change from other servers.
 *
 * @author liuzunfei
 * @version $Id: ConfigChangeClusterSyncRequestHandler.java, v 0.1 2020年08月11日 4:35 PM liuzunfei Exp $
 */
@Component
public class ConfigChangeClusterSyncRequestHandler
        extends RequestHandler<ConfigChangeClusterSyncRequest, ConfigChangeClusterSyncResponse> {
    
    private final DumpService dumpService;
    
    public ConfigChangeClusterSyncRequestHandler(DumpService dumpService) {
        this.dumpService = dumpService;
    }
    
    @TpsControl(pointName = "ClusterConfigChangeNotify")
    @Override
    public ConfigChangeClusterSyncResponse handle(ConfigChangeClusterSyncRequest configChangeSyncRequest,
            RequestMeta meta) throws NacosException {
        // 调用到其他节点，其他节点也是执行dump服务，然后通知和本机连接的客户端，通知他们进行配置更新。
        if (configChangeSyncRequest.isBeta()) {
            dumpService.dumpBeta(configChangeSyncRequest.getDataId(), configChangeSyncRequest.getGroup(),
                    configChangeSyncRequest.getTenant(), configChangeSyncRequest.getLastModified(), meta.getClientIp());
        } else if (configChangeSyncRequest.isBatch()) {
            dumpService.dumpBatch(configChangeSyncRequest.getDataId(), configChangeSyncRequest.getGroup(),
                    configChangeSyncRequest.getTenant(), configChangeSyncRequest.getLastModified(), meta.getClientIp());
        } else if (StringUtils.isNotBlank(configChangeSyncRequest.getTag())) {
            dumpService.dumpTag(configChangeSyncRequest.getDataId(), configChangeSyncRequest.getGroup(),
                    configChangeSyncRequest.getTenant(), configChangeSyncRequest.getTag(),
                    configChangeSyncRequest.getLastModified(), meta.getClientIp());
        } else {
            // 本机的dump服务
            dumpService.dumpFormal(configChangeSyncRequest.getDataId(), configChangeSyncRequest.getGroup(),
                    configChangeSyncRequest.getTenant(), configChangeSyncRequest.getLastModified(), meta.getClientIp());
        }
        return new ConfigChangeClusterSyncResponse();
    }
    
}
