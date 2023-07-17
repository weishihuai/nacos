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

package com.alibaba.nacos.client.config;

import com.alibaba.nacos.api.PropertyKeyConst;
import com.alibaba.nacos.api.common.Constants;
import com.alibaba.nacos.api.config.ConfigService;
import com.alibaba.nacos.api.config.ConfigType;
import com.alibaba.nacos.api.config.filter.IConfigFilter;
import com.alibaba.nacos.api.config.listener.Listener;
import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.client.config.filter.impl.ConfigFilterChainManager;
import com.alibaba.nacos.client.config.filter.impl.ConfigRequest;
import com.alibaba.nacos.client.config.filter.impl.ConfigResponse;
import com.alibaba.nacos.client.config.http.ServerHttpAgent;
import com.alibaba.nacos.client.config.impl.ClientWorker;
import com.alibaba.nacos.client.config.impl.LocalConfigInfoProcessor;
import com.alibaba.nacos.client.config.impl.LocalEncryptedDataKeyProcessor;
import com.alibaba.nacos.client.config.impl.ServerListManager;
import com.alibaba.nacos.client.config.utils.ContentUtils;
import com.alibaba.nacos.client.config.utils.ParamUtils;
import com.alibaba.nacos.client.env.NacosClientProperties;
import com.alibaba.nacos.client.utils.LogUtils;
import com.alibaba.nacos.client.utils.ParamUtil;
import com.alibaba.nacos.client.utils.ValidatorUtils;
import com.alibaba.nacos.common.utils.StringUtils;
import org.slf4j.Logger;

import java.util.Arrays;
import java.util.Properties;

/**
 * Config Impl.
 *
 * @author Nacos
 */
@SuppressWarnings("PMD.ServiceOrDaoClassShouldEndWithImplRule")
public class NacosConfigService implements ConfigService {

    private static final Logger LOGGER = LogUtils.logger(NacosConfigService.class);

    private static final String UP = "UP";

    private static final String DOWN = "DOWN";

    /**
     * will be deleted in 2.0 later versions
     */
    @Deprecated
    ServerHttpAgent agent = null;

    /**
     * 长轮训
     */
    private final ClientWorker worker;

    private String namespace;

    private final ConfigFilterChainManager configFilterChainManager;

    public NacosConfigService(Properties properties) throws NacosException {
        final NacosClientProperties clientProperties = NacosClientProperties.PROTOTYPE.derive(properties);
        ValidatorUtils.checkInitParam(clientProperties);

        // 初始化Namespace
        initNamespace(clientProperties);
        // 创建了一个配置过滤器链，可以采用SPI扩展机制加载对应的过滤器实现类
        this.configFilterChainManager = new ConfigFilterChainManager(clientProperties.asProperties());
        // 创建了一个服务管理器，内含一个定时轮询线程池，每隔30s拉取一次服务
        ServerListManager serverListManager = new ServerListManager(clientProperties);
        serverListManager.start();
        // 创建了一个客户端工作者，包含了一个代理对象
        this.worker = new ClientWorker(this.configFilterChainManager, serverListManager, clientProperties);
        // will be deleted in 2.0 later versions
        agent = new ServerHttpAgent(serverListManager);

    }

    private void initNamespace(NacosClientProperties properties) {
        namespace = ParamUtil.parseNamespace(properties);
        properties.setProperty(PropertyKeyConst.NAMESPACE, namespace);
    }

    @Override
    public String getConfig(String dataId, String group, long timeoutMs) throws NacosException {
        return getConfigInner(namespace, dataId, group, timeoutMs);
    }

    @Override
    public String getConfigAndSignListener(String dataId, String group, long timeoutMs, Listener listener) throws NacosException {
        group = StringUtils.isBlank(group) ? Constants.DEFAULT_GROUP : group.trim();
        ConfigResponse configResponse = worker.getAgent().queryConfig(dataId, group, worker.getAgent().getTenant(), timeoutMs, false);
        String content = configResponse.getContent();
        String encryptedDataKey = configResponse.getEncryptedDataKey();
        worker.addTenantListenersWithContent(dataId, group, content, encryptedDataKey, Arrays.asList(listener));

        // get a decryptContent, fix https://github.com/alibaba/nacos/issues/7039
        ConfigResponse cr = new ConfigResponse();
        cr.setDataId(dataId);
        cr.setGroup(group);
        cr.setContent(content);
        cr.setEncryptedDataKey(encryptedDataKey);
        configFilterChainManager.doFilter(null, cr);
        return cr.getContent();
    }

    @Override
    public void addListener(String dataId, String group, Listener listener) throws NacosException {
        // 配置监听：监听采用的回调的思想，当服务端通知的时候，调用回调方法
        worker.addTenantListeners(dataId, group, Arrays.asList(listener));
    }

    @Override
    public boolean publishConfig(String dataId, String group, String content) throws NacosException {
        return publishConfig(dataId, group, content, ConfigType.getDefaultType().getType());
    }

    @Override
    public boolean publishConfig(String dataId, String group, String content, String type) throws NacosException {
        return publishConfigInner(namespace, dataId, group, null, null, null, content, type, null);
    }

    @Override
    public boolean publishConfigCas(String dataId, String group, String content, String casMd5) throws NacosException {
        return publishConfigInner(namespace, dataId, group, null, null, null, content, ConfigType.getDefaultType().getType(), casMd5);
    }

    @Override
    public boolean publishConfigCas(String dataId, String group, String content, String casMd5, String type) throws NacosException {
        return publishConfigInner(namespace, dataId, group, null, null, null, content, type, casMd5);
    }

    @Override
    public boolean removeConfig(String dataId, String group) throws NacosException {
        return removeConfigInner(namespace, dataId, group, null);
    }

    @Override
    public void removeListener(String dataId, String group, Listener listener) {
        worker.removeTenantListener(dataId, group, listener);
    }

    /**
     * 获取配置内容
     * <pre>
     *  1.从本地失败转移的文件夹中获取配置，这个是手工添加的，程序不会自动处理，是针对一些特定情况下，比如服务挂了还需要修改本地的配置的情况。
     *  给了一次本地修改处理的方式，也算是预留了一个备案，防止一些极端情况。但是这个得了解其固定的目录和拉取的配置，再处理，用完了要及时删除，
     *  否则会一直拉取本地的文件，毕竟是优先处理这部分的逻辑
     *  2.去服务端拉取，这个就是正常逻辑，获取服务端存储的配置信息
     *  3. 对于出现了比如超时的情况，在有本地快照的情况的，从本地快照拉取配置，不至于偶尔超时了就配置没了
     * </pre>
     *
     * @param tenant    命名空间
     * @param dataId    配置ID
     * @param group     组名
     * @param timeoutMs 超时时间
     * @return
     * @throws NacosException
     */
    private String getConfigInner(String tenant, String dataId, String group, long timeoutMs) throws NacosException {
        // 默认组名：DEFAULT_GROUP
        group = blank2defaultGroup(group);
        // 校验组名、dataId
        ParamUtils.checkKeyParam(dataId, group);
        ConfigResponse cr = new ConfigResponse();
        // 设置配置信息
        cr.setDataId(dataId);
        cr.setTenant(tenant);
        cr.setGroup(group);

        // We first try to use local failover content if exists.
        // A config content for failover is not created by client program automatically,
        // but is maintained by user.
        // This is designed for certain scenario like client emergency reboot,
        // changing config needed in the same time, while nacos server is down.

        // 首先尝试使用本地如果存在的故障转移的配置内容，如果能读到失败转移的配置信息，则直接返回了。
        // 优先使用失败转移，设计的目的是当server挂后，又需要修改配置，就可以读本地目录
        String content = LocalConfigInfoProcessor.getFailover(worker.getAgentName(), dataId, group, tenant);
        if (content != null) {
            LOGGER.warn("[{}] [get-config] get failover ok, dataId={}, group={}, tenant={}, config={}", worker.getAgentName(), dataId, group, tenant, ContentUtils.truncateContent(content));
            cr.setContent(content);
            String encryptedDataKey = LocalEncryptedDataKeyProcessor.getEncryptDataKeyFailover(agent.getName(), dataId, group, tenant);
            cr.setEncryptedDataKey(encryptedDataKey);
            configFilterChainManager.doFilter(null, cr);
            content = cr.getContent();
            return content;
        }

        try {
            // 去服务端拉取，这个就是正常逻辑，获取服务端存储的配置信息
            ConfigResponse response = worker.getServerConfig(dataId, group, tenant, timeoutMs, false);
            cr.setContent(response.getContent());
            cr.setEncryptedDataKey(response.getEncryptedDataKey());
            configFilterChainManager.doFilter(null, cr);
            content = cr.getContent();

            return content;
        } catch (NacosException ioe) {
            if (NacosException.NO_RIGHT == ioe.getErrCode()) {
                throw ioe;
            }
            LOGGER.warn("[{}] [get-config] get from server error, dataId={}, group={}, tenant={}, msg={}", worker.getAgentName(), dataId, group, tenant, ioe.toString());
        }

        // 非鉴权失败的异常（NacosException.NO_RIGHT）的，尝试从本地快照中获取配置 (例如超时，不至于偶尔超时了，就无法获取到配置内容，)
        content = LocalConfigInfoProcessor.getSnapshot(worker.getAgentName(), dataId, group, tenant);
        if (content != null) {
            LOGGER.warn("[{}] [get-config] get snapshot ok, dataId={}, group={}, tenant={}, config={}", worker.getAgentName(), dataId, group, tenant, ContentUtils.truncateContent(content));
        }
        cr.setContent(content);
        String encryptedDataKey = LocalEncryptedDataKeyProcessor.getEncryptDataKeySnapshot(agent.getName(), dataId, group, tenant);
        cr.setEncryptedDataKey(encryptedDataKey);
        configFilterChainManager.doFilter(null, cr);
        content = cr.getContent();
        return content;
    }

    private String blank2defaultGroup(String group) {
        return (StringUtils.isBlank(group)) ? Constants.DEFAULT_GROUP : group.trim();
    }

    private boolean removeConfigInner(String tenant, String dataId, String group, String tag) throws NacosException {
        group = blank2defaultGroup(group);
        ParamUtils.checkKeyParam(dataId, group);
        return worker.removeConfig(dataId, group, tenant, tag);
    }

    private boolean publishConfigInner(String tenant, String dataId, String group, String tag, String appName, String betaIps, String content, String type, String casMd5) throws NacosException {
        group = blank2defaultGroup(group);
        ParamUtils.checkParam(dataId, group, content);

        ConfigRequest cr = new ConfigRequest();
        cr.setDataId(dataId);
        cr.setTenant(tenant);
        cr.setGroup(group);
        cr.setContent(content);
        cr.setType(type);
        configFilterChainManager.doFilter(cr, null);
        content = cr.getContent();
        String encryptedDataKey = cr.getEncryptedDataKey();

        return worker.publishConfig(dataId, group, tenant, appName, tag, betaIps, content, encryptedDataKey, casMd5, type);
    }

    @Override
    public String getServerStatus() {
        if (worker.isHealthServer()) {
            return UP;
        } else {
            return DOWN;
        }
    }

    @Override
    public void addConfigFilter(IConfigFilter configFilter) {
        configFilterChainManager.addFilter(configFilter);
    }

    @Override
    public void shutDown() throws NacosException {
        worker.shutdown();
    }
}
