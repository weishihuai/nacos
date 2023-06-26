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

package com.alibaba.nacos.core.remote;

import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.remote.request.Request;
import com.alibaba.nacos.api.remote.request.RequestMeta;
import com.alibaba.nacos.api.remote.response.Response;
import com.alibaba.nacos.core.utils.Loggers;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Nacos based request handler.
 *
 * @author liuzunfei
 * @author xiweng.yy
 */
@SuppressWarnings("PMD.AbstractClassShouldStartWithAbstractNamingRule")
public abstract class RequestHandler<T extends Request, S extends Response> {
    
    @Autowired
    private RequestFilters requestFilters;
    
    /**
     * Handler request.
     *
     * @param request request
     * @param meta    request meta data
     * @return response
     * @throws NacosException nacos exception when handle request has problem.
     */
    public Response handleRequest(T request, RequestMeta meta) throws NacosException {
        for (AbstractRequestFilter filter : requestFilters.filters) {
            try {
                Response filterResult = filter.filter(request, meta, this.getClass());
                if (filterResult != null && !filterResult.isSuccess()) {
                    return filterResult;
                }
            } catch (Throwable throwable) {
                Loggers.REMOTE.error("filter error", throwable);
            }
            
        }
        // 调用处理方法
        return handle(request, meta);
    }
    
    /**
     * 抽象方法，由子类处理。可以通过参数中的泛型，推导出具体是哪个处理类
     *
     * 例如：服务注册请求对象是：{@link com.alibaba.nacos.api.naming.remote.request.InstanceRequest}
     * 那么对应的处理类就是：{@link com.alibaba.nacos.naming.remote.rpc.handler.InstanceRequestHandler#handle(com.alibaba.nacos.api.naming.remote.request.InstanceRequest, com.alibaba.nacos.api.remote.request.RequestMeta)}
     *
     * Handler request.
     *
     * @param request request
     * @param meta    request meta data
     * @return response
     * @throws NacosException nacos exception when handle request has problem.
     */
    public abstract S handle(T request, RequestMeta meta) throws NacosException;
    
}
