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

package com.alibaba.nacos.config.server.service.dump;

import com.alibaba.nacos.common.utils.StringUtils;
import com.alibaba.nacos.common.notify.Event;
import com.alibaba.nacos.common.notify.listener.Subscriber;
import com.alibaba.nacos.config.server.model.event.ConfigDumpEvent;
import com.alibaba.nacos.config.server.service.AggrWhitelist;
import com.alibaba.nacos.config.server.service.ClientIpWhiteList;
import com.alibaba.nacos.config.server.service.ConfigCacheService;
import com.alibaba.nacos.config.server.service.SwitchService;
import com.alibaba.nacos.config.server.service.trace.ConfigTraceService;

/**
 * Dump config subscriber.
 *
 * @author <a href="mailto:liaochuntao@live.com">liaochuntao</a>
 */
public class DumpConfigHandler extends Subscriber<ConfigDumpEvent> {
    
    /**
     * trigger config dump event.
     *
     * @param event {@link ConfigDumpEvent}
     * @return {@code true} if the config dump task success , else {@code false}
     */
    public static boolean configDump(ConfigDumpEvent event) {
        final String dataId = event.getDataId();
        final String group = event.getGroup();
        final String namespaceId = event.getNamespaceId();
        final String content = event.getContent();
        final String type = event.getType();
        final long lastModified = event.getLastModifiedTs();
        //beta测试版
        if (event.isBeta()) {
            boolean result = false;
            if (event.isRemove()) {
                result = ConfigCacheService.removeBeta(dataId, group, namespaceId);
                if (result) {
                    ConfigTraceService.logDumpBetaEvent(dataId, group, namespaceId, null, lastModified,
                            event.getHandleIp(), ConfigTraceService.DUMP_TYPE_REMOVE_OK,
                            System.currentTimeMillis() - lastModified, 0);
                }
                return result;
            } else {
                result = ConfigCacheService.dumpBeta(dataId, group, namespaceId, content, lastModified,
                        event.getBetaIps(), event.getEncryptedDataKey());
                if (result) {
                    ConfigTraceService.logDumpBetaEvent(dataId, group, namespaceId, null, lastModified,
                            event.getHandleIp(), ConfigTraceService.DUMP_TYPE_OK,
                            System.currentTimeMillis() - lastModified, content.length());
                }
            }
            
            return result;
        }
        
        //tag不为空的处理
        if (StringUtils.isNotBlank(event.getTag())) {
            //
            boolean result;
            if (!event.isRemove()) {
                // 非删除配置事件
                result = ConfigCacheService.dumpTag(dataId, group, namespaceId, event.getTag(), content, lastModified,
                        event.getEncryptedDataKey());
                if (result) {
                    ConfigTraceService.logDumpTagEvent(dataId, group, namespaceId, event.getTag(), null, lastModified,
                            event.getHandleIp(), ConfigTraceService.DUMP_TYPE_OK,
                            System.currentTimeMillis() - lastModified, content.length());
                }
            } else {
                // 删除配置事件，移除配置缓存
                result = ConfigCacheService.removeTag(dataId, group, namespaceId, event.getTag());
                if (result) {
                    ConfigTraceService.logDumpTagEvent(dataId, group, namespaceId, event.getTag(), null, lastModified,
                            event.getHandleIp(), ConfigTraceService.DUMP_TYPE_REMOVE_OK,
                            System.currentTimeMillis() - lastModified, 0);
                }
            }
            return result;
        }
        
        // 内置的一些特殊配置
        if (dataId.equals(AggrWhitelist.AGGRIDS_METADATA)) {
            AggrWhitelist.load(content);
        }
        
        if (dataId.equals(ClientIpWhiteList.CLIENT_IP_WHITELIST_METADATA)) {
            ClientIpWhiteList.load(content);
        }
        
        if (dataId.equals(SwitchService.SWITCH_META_DATAID)) {
            SwitchService.load(content);
        }
        
        boolean result;
        if (!event.isRemove()) {
            // 非删除事件：配置缓存服务dump配置信息
            result = ConfigCacheService.dump(dataId, group, namespaceId, content, lastModified, event.getType(),
                    event.getEncryptedDataKey());
            
            if (result) {
                // 记录日志
                ConfigTraceService.logDumpEvent(dataId, group, namespaceId, null, lastModified, event.getHandleIp(),
                        ConfigTraceService.DUMP_TYPE_OK, System.currentTimeMillis() - lastModified, content.length());
            }
        } else {
            // 删除配置事件，移除配置缓存
            result = ConfigCacheService.remove(dataId, group, namespaceId);
            
            if (result) {
                // 记录日志
                ConfigTraceService.logDumpEvent(dataId, group, namespaceId, null, lastModified, event.getHandleIp(),
                        ConfigTraceService.DUMP_TYPE_REMOVE_OK, System.currentTimeMillis() - lastModified, 0);
            }
        }
        return result;
        
    }
    
    @Override
    public void onEvent(ConfigDumpEvent event) {
        configDump(event);
    }
    
    @Override
    public Class<? extends Event> subscribeType() {
        return ConfigDumpEvent.class;
    }
}
