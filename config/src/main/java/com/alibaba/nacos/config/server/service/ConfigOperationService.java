/*
 * Copyright 1999-2022 Alibaba Group Holding Ltd.
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

package com.alibaba.nacos.config.server.service;

import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.exception.api.NacosApiException;
import com.alibaba.nacos.api.model.v2.ErrorCode;
import com.alibaba.nacos.common.utils.MapUtil;
import com.alibaba.nacos.common.utils.StringUtils;
import com.alibaba.nacos.config.server.model.ConfigInfo;
import com.alibaba.nacos.config.server.model.ConfigOperateResult;
import com.alibaba.nacos.config.server.model.event.ConfigDataChangeEvent;
import com.alibaba.nacos.config.server.model.ConfigRequestInfo;
import com.alibaba.nacos.config.server.model.form.ConfigForm;
import com.alibaba.nacos.config.server.service.repository.ConfigInfoBetaPersistService;
import com.alibaba.nacos.config.server.service.repository.ConfigInfoPersistService;
import com.alibaba.nacos.config.server.service.repository.ConfigInfoTagPersistService;
import com.alibaba.nacos.config.server.service.trace.ConfigTraceService;
import com.alibaba.nacos.config.server.utils.ParamUtils;
import com.alibaba.nacos.config.server.utils.TimeUtils;
import com.alibaba.nacos.sys.utils.InetUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

/**
 * ConfigService.
 *
 * @author dongyafei
 * @date 2022/8/11
 */

@Service
public class ConfigOperationService {
    
    private ConfigInfoPersistService configInfoPersistService;
    
    private ConfigInfoTagPersistService configInfoTagPersistService;
    
    private ConfigInfoBetaPersistService configInfoBetaPersistService;
    
    private static final Logger LOGGER = LoggerFactory.getLogger(ConfigOperationService.class);
    
    public ConfigOperationService(ConfigInfoPersistService configInfoPersistService,
            ConfigInfoTagPersistService configInfoTagPersistService,
            ConfigInfoBetaPersistService configInfoBetaPersistService) {
        this.configInfoPersistService = configInfoPersistService;
        this.configInfoTagPersistService = configInfoTagPersistService;
        this.configInfoBetaPersistService = configInfoBetaPersistService;
    }
    
    /**
     * Adds or updates non-aggregated data.
     *
     * @throws NacosException NacosException.
     */
    public Boolean publishConfig(ConfigForm configForm, ConfigRequestInfo configRequestInfo, String encryptedDataKey)
            throws NacosException {
        // 将配置高级信息转成Map键值对
        Map<String, Object> configAdvanceInfo = getConfigAdvanceInfo(configForm);
        // 检查参数
        ParamUtils.checkParam(configAdvanceInfo);
        
        if (AggrWhitelist.isAggrDataId(configForm.getDataId())) {
            LOGGER.warn("[aggr-conflict] {} attempt to publish single data, {}, {}", configRequestInfo.getSrcIp(),
                    configForm.getDataId(), configForm.getGroup());
            throw new NacosApiException(HttpStatus.FORBIDDEN.value(), ErrorCode.INVALID_DATA_ID,
                    "dataId:" + configForm.getDataId() + " is aggr");
        }

        // 构建ConfigInfo配置信息，发布配置最基本的五个参数： nameSpaceId、groupId、dataId、应用名称、配置内容
        ConfigInfo configInfo = new ConfigInfo(configForm.getDataId(), configForm.getGroup(),
                configForm.getNamespaceId(), configForm.getAppName(), configForm.getContent());
        
        configInfo.setType(configForm.getType());
        configInfo.setEncryptedDataKey(encryptedDataKey);
        ConfigOperateResult configOperateResult = null;
        
        String persistEvent = ConfigTraceService.PERSISTENCE_EVENT;

        // 判断是否是beta测试版本
        if (StringUtils.isBlank(configRequestInfo.getBetaIps())) {
            // 正常发布，大部分情况下，我们都没有指定tag
            if (StringUtils.isBlank(configForm.getTag())) {
                // 1、插入 or 更新配置信息
                // 这里分为内置数据库（EmbeddedConfigInfoPersistServiceImpl）和外置数据库（ExternalConfigInfoPersistServiceImpl）操作，通常我们都是使用MySQL进行持久化存储
                configOperateResult = configInfoPersistService.insertOrUpdate(configRequestInfo.getSrcIp(),
                        configForm.getSrcUser(), configInfo, configAdvanceInfo);

                // 2、发布配置数据变动事件
                ConfigChangePublisher.notifyConfigChange(
                        new ConfigDataChangeEvent(false, configForm.getDataId(), configForm.getGroup(),
                                configForm.getNamespaceId(), configOperateResult.getLastModified()));
            } else {
                // 指定tag
                persistEvent = ConfigTraceService.PERSISTENCE_EVENT_TAG + "-" + configForm.getTag();
                configOperateResult = configInfoTagPersistService.insertOrUpdateTag(configInfo, configForm.getTag(),
                        configRequestInfo.getSrcIp(), configForm.getSrcUser());
                ConfigChangePublisher.notifyConfigChange(
                        new ConfigDataChangeEvent(false, configForm.getDataId(), configForm.getGroup(),
                                configForm.getNamespaceId(), configForm.getTag(),
                                configOperateResult.getLastModified()));
            }
        } else {
            persistEvent = ConfigTraceService.PERSISTENCE_EVENT_BETA;
            
            // beta publish
            configOperateResult = configInfoBetaPersistService.insertOrUpdateBeta(configInfo,
                    configRequestInfo.getBetaIps(), configRequestInfo.getSrcIp(), configForm.getSrcUser());
            ConfigChangePublisher.notifyConfigChange(
                    new ConfigDataChangeEvent(true, configForm.getDataId(), configForm.getGroup(),
                            configForm.getNamespaceId(), configOperateResult.getLastModified()));
        }
        // 日志跟踪
        ConfigTraceService.logPersistenceEvent(configForm.getDataId(), configForm.getGroup(),
                configForm.getNamespaceId(), configRequestInfo.getRequestIpApp(), configOperateResult.getLastModified(),
                InetUtils.getSelfIP(), persistEvent, ConfigTraceService.PERSISTENCE_TYPE_PUB, configForm.getContent());
        
        return true;
    }
    
    /**
     * Synchronously delete all pre-aggregation data under a dataId.
     */
    public Boolean deleteConfig(String dataId, String group, String namespaceId, String tag, String clientIp,
            String srcUser) {
        String persistEvent = ConfigTraceService.PERSISTENCE_EVENT;
        if (StringUtils.isBlank(tag)) {
            configInfoPersistService.removeConfigInfo(dataId, group, namespaceId, clientIp, srcUser);
        } else {
            persistEvent = ConfigTraceService.PERSISTENCE_EVENT_TAG + "-" + tag;
            configInfoTagPersistService.removeConfigInfoTag(dataId, group, namespaceId, tag, clientIp, srcUser);
        }
        final Timestamp time = TimeUtils.getCurrentTime();
        ConfigTraceService.logPersistenceEvent(dataId, group, namespaceId, null, time.getTime(), clientIp, persistEvent,
                ConfigTraceService.PERSISTENCE_TYPE_REMOVE, null);
        ConfigChangePublisher.notifyConfigChange(
                new ConfigDataChangeEvent(false, dataId, group, namespaceId, tag, time.getTime()));
        
        return true;
    }
    
    public Map<String, Object> getConfigAdvanceInfo(ConfigForm configForm) {
        Map<String, Object> configAdvanceInfo = new HashMap<>(10);
        MapUtil.putIfValNoNull(configAdvanceInfo, "config_tags", configForm.getConfigTags());
        MapUtil.putIfValNoNull(configAdvanceInfo, "desc", configForm.getDesc());
        MapUtil.putIfValNoNull(configAdvanceInfo, "use", configForm.getUse());
        MapUtil.putIfValNoNull(configAdvanceInfo, "effect", configForm.getEffect());
        MapUtil.putIfValNoNull(configAdvanceInfo, "type", configForm.getType());
        MapUtil.putIfValNoNull(configAdvanceInfo, "schema", configForm.getSchema());
        return configAdvanceInfo;
    }
}
